package io.akasb.taskplatform.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.akasb.taskplatform.api.IdempotencyCache;
import io.akasb.taskplatform.api.InMemoryRateLimiter;
import io.akasb.taskplatform.api.JobService;
import io.akasb.taskplatform.api.RateLimitFilter;
import io.akasb.taskplatform.api.RequestBodyLimitFilter;
import io.akasb.taskplatform.api.RateLimiter;
import io.akasb.taskplatform.dispatch.Checkpoints;
import io.akasb.taskplatform.dispatch.InMemoryDispatcher;
import io.akasb.taskplatform.dispatch.JobDispatcher;
import io.akasb.taskplatform.dispatch.JobLifecycle;
import io.akasb.taskplatform.dispatch.Reconciler;
import io.akasb.taskplatform.domain.RetryPolicy;
import io.akasb.taskplatform.observability.LifecycleListener;
import io.akasb.taskplatform.observability.MicrometerJobMetrics;
import io.akasb.taskplatform.persistence.JdbcJobRepository;
import io.akasb.taskplatform.persistence.JobRepository;
import io.akasb.taskplatform.worker.LocalWorkerProtocol;
import io.akasb.taskplatform.worker.TaskHandler;
import io.akasb.taskplatform.worker.TaskHandlerRegistry;
import io.akasb.taskplatform.worker.WorkerProtocol;
import io.akasb.taskplatform.worker.handlers.FlakyTaskHandler;
import io.akasb.taskplatform.worker.handlers.SleepTaskHandler;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

/** Wires the framework-free core (domain, persistence, dispatch, worker) into Spring. */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(TaskPlatformProperties.class)
public class PlatformConfiguration {
    /** Bytes allowed in a submission body on top of the payload limit (type, queue, limits, JSON syntax). */
    static final int REQUEST_ENVELOPE_BYTES = 16 * 1024;

    /** Prometheus: {@code taskplatform_ratelimit_rejected_total}. */
    public static final String RATE_LIMIT_REJECTED = "taskplatform.ratelimit.rejected";
    private static final Logger log = LoggerFactory.getLogger(PlatformConfiguration.class);

    @Bean
    @ConditionalOnMissingBean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    RetryPolicy retryPolicy(TaskPlatformProperties props) {
        TaskPlatformProperties.Retry r = props.getRetry();
        Random random = r.getSeed() == null ? new Random() : new Random(r.getSeed());
        return new RetryPolicy(r.getInitialDelay(), r.getMultiplier(), r.getMaxDelay(), r.getJitter(), random);
    }

    @Bean
    JobRepository jobRepository(DataSource dataSource) {
        return new JdbcJobRepository(dataSource);
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "taskplatform.dispatcher", name = "type", havingValue = "in-memory",
            matchIfMissing = true)
    JobDispatcher inMemoryDispatcher() {
        return new InMemoryDispatcher();
    }

    @Bean
    MicrometerJobMetrics jobMetrics(MeterRegistry registry, TaskPlatformProperties props) {
        MicrometerJobMetrics metrics = new MicrometerJobMetrics(registry);
        props.getWorkers().getPools().stream().map(TaskPlatformProperties.Pool::getQueue).distinct()
                .forEach(metrics::registerQueue);
        return metrics;
    }

    @Bean
    @ConditionalOnMissingBean
    Checkpoints checkpoints() {
        return Checkpoints.none();
    }

    @Bean
    JobLifecycle jobLifecycle(JobRepository repository, JobDispatcher dispatcher, RetryPolicy retryPolicy,
                              Clock clock, TaskPlatformProperties props, ObjectProvider<LifecycleListener> listeners,
                              Checkpoints checkpoints) {
        return new JobLifecycle(repository, dispatcher, retryPolicy, clock, props.getLeaseDuration(),
                LifecycleListener.composite(listeners.orderedStream().toList()), checkpoints);
    }

    @Bean
    Reconciler reconciler(JobRepository repository, JobLifecycle lifecycle, JobDispatcher dispatcher,
                          TaskPlatformProperties props) {
        TaskPlatformProperties.Reconciler r = props.getReconciler();
        return new Reconciler(repository, lifecycle, dispatcher, new Reconciler.Settings(r.getLeaseGrace(),
                r.getPendingGrace(), r.getRedispatchAfter(), r.getBatchSize()));
    }

    @Bean
    ReconcilerRunner reconcilerRunner(Reconciler reconciler, TaskPlatformProperties props) {
        return new ReconcilerRunner(reconciler, props.getReconciler());
    }

    @Bean
    SleepTaskHandler sleepTaskHandler() {
        return new SleepTaskHandler();
    }

    @Bean
    FlakyTaskHandler flakyTaskHandler() {
        return new FlakyTaskHandler();
    }

    @Bean
    TaskHandlerRegistry taskHandlerRegistry(List<TaskHandler> handlers) {
        return new TaskHandlerRegistry(handlers);
    }

    @Bean
    WorkerProtocol workerProtocol(JobDispatcher dispatcher, JobLifecycle lifecycle) {
        return new LocalWorkerProtocol(dispatcher, lifecycle);
    }

    @Bean
    WorkerPools workerPools(TaskPlatformProperties props, WorkerProtocol protocol, TaskHandlerRegistry handlers,
                            Clock clock, ObjectMapper mapper, MeterRegistry registry, JobDispatcher dispatcher) {
        return new WorkerPools(props, protocol, handlers, clock, mapper, registry, dispatcher);
    }

    /** The idempotency cache is optional ({@link RedisConfiguration}); without one every keyed submission hits SQL. */
    @Bean
    JobService jobService(JobLifecycle lifecycle, JobRepository repository, TaskHandlerRegistry handlers,
                          TaskPlatformProperties props, ObjectMapper mapper,
                          ObjectProvider<IdempotencyCache> idempotencyCache) {
        TaskPlatformProperties.Api api = props.getApi();
        Set<String> served = api.isRequireServedQueue()
                ? props.getWorkers().getPools().stream().map(TaskPlatformProperties.Pool::getQueue)
                .collect(Collectors.toUnmodifiableSet())
                : Set.of();
        JobService.Limits limits = new JobService.Limits(props.getRetry().getDefaultMaxAttempts(),
                api.getMaxAttemptsLimit(), api.getDefaultTimeout(), api.getMaxTimeout(), api.getMaxPayloadBytes());
        return new JobService(lifecycle, repository, handlers::types, () -> served, limits, mapper,
                idempotencyCache.getIfAvailable(IdempotencyCache::none));
    }

    /**
     * Rate limit on {@code POST /v1/jobs}: shared counters in Redis when {@link RedisConfiguration} provides a
     * {@link RateLimiter}, otherwise a per-instance in-memory limiter. Runs after the HTTP observation filter, so
     * rejected requests still show up in {@code http_server_requests}.
     */
    @Bean
    @ConditionalOnProperty(prefix = "taskplatform.rate-limit", name = "enabled", havingValue = "true")
    FilterRegistrationBean<RateLimitFilter> rateLimitFilter(TaskPlatformProperties props,
                                                            ObjectProvider<RateLimiter> rateLimiter, Clock clock,
                                                            MeterRegistry registry) {
        TaskPlatformProperties.RateLimit rl = props.getRateLimit();
        RateLimiter limiter = rateLimiter.getIfAvailable(
                () -> new InMemoryRateLimiter(rl.getRequestsPerWindow(), rl.getWindow(), clock));
        log.info("rate limit on job submission: {} requests per {} ms per client ({})", rl.getRequestsPerWindow(),
                rl.getWindow().toMillis(), limiter instanceof InMemoryRateLimiter ? "per instance, in memory"
                        : "shared, " + limiter.getClass().getSimpleName());
        Counter rejected = Counter.builder(RATE_LIMIT_REJECTED)
                .description("Job submissions rejected with 429 by the rate limit").register(registry);
        FilterRegistrationBean<RateLimitFilter> registration =
                new FilterRegistrationBean<>(new RateLimitFilter(limiter, rejected::increment));
        registration.setName("rateLimitFilter");
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE + 10);
        return registration;
    }

    /** Rejects oversized submissions (413) before parsing: the payload limit plus room for the other fields. */
    @Bean
    FilterRegistrationBean<RequestBodyLimitFilter> requestBodyLimitFilter(TaskPlatformProperties props) {
        FilterRegistrationBean<RequestBodyLimitFilter> registration = new FilterRegistrationBean<>(
                new RequestBodyLimitFilter(props.getApi().getMaxPayloadBytes() + REQUEST_ENVELOPE_BYTES));
        registration.setName("requestBodyLimitFilter");
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE + 20);
        return registration;
    }
}
