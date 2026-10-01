package io.akasb.taskplatform.config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** All {@code taskplatform.*} settings. Defaults here; overrides in application.yml or environment variables. */
@ConfigurationProperties(prefix = "taskplatform")
public class TaskPlatformProperties {
    /** How long a lease is valid without a heartbeat. Failover time for a crashed worker ≈ lease + grace + scan. */
    private Duration leaseDuration = Duration.ofSeconds(30);
    /** How often a running attempt renews its lease; keep at or below a third of the lease duration. */
    private Duration heartbeatInterval = Duration.ofSeconds(10);
    private final Retry retry = new Retry();
    private final Reconciler reconciler = new Reconciler();
    private final Workers workers = new Workers();
    private final Api api = new Api();
    private final Dispatcher dispatcher = new Dispatcher();
    private final Rabbitmq rabbitmq = new Rabbitmq();
    private final Redis redis = new Redis();
    private final RateLimit rateLimit = new RateLimit();

    public Duration getLeaseDuration() { return leaseDuration; }
    public void setLeaseDuration(Duration leaseDuration) { this.leaseDuration = leaseDuration; }
    public Duration getHeartbeatInterval() { return heartbeatInterval; }
    public void setHeartbeatInterval(Duration heartbeatInterval) { this.heartbeatInterval = heartbeatInterval; }
    public Retry getRetry() { return retry; }
    public Reconciler getReconciler() { return reconciler; }
    public Workers getWorkers() { return workers; }
    public Api getApi() { return api; }
    public Dispatcher getDispatcher() { return dispatcher; }
    public Rabbitmq getRabbitmq() { return rabbitmq; }
    public Redis getRedis() { return redis; }
    public RateLimit getRateLimit() { return rateLimit; }

    public static class Retry {
        private Duration initialDelay = Duration.ofSeconds(1);
        private double multiplier = 2.0;
        private Duration maxDelay = Duration.ofMinutes(1);
        /** Fraction of the base delay removed at random: delay in [(1 - jitter) * base, base]. */
        private double jitter = 0.5;
        /** Fixed seed for reproducible jitter (tests, benchmarks); random when unset. */
        private Long seed;
        /** Total attempts (first attempt included) when a request does not say. */
        private int defaultMaxAttempts = 3;

        public Duration getInitialDelay() { return initialDelay; }
        public void setInitialDelay(Duration initialDelay) { this.initialDelay = initialDelay; }
        public double getMultiplier() { return multiplier; }
        public void setMultiplier(double multiplier) { this.multiplier = multiplier; }
        public Duration getMaxDelay() { return maxDelay; }
        public void setMaxDelay(Duration maxDelay) { this.maxDelay = maxDelay; }
        public double getJitter() { return jitter; }
        public void setJitter(double jitter) { this.jitter = jitter; }
        public Long getSeed() { return seed; }
        public void setSeed(Long seed) { this.seed = seed; }
        public int getDefaultMaxAttempts() { return defaultMaxAttempts; }
        public void setDefaultMaxAttempts(int defaultMaxAttempts) { this.defaultMaxAttempts = defaultMaxAttempts; }
    }

    public static class Reconciler {
        private boolean enabled = true;
        /** Delay between reconciler passes (also the retry release granularity). */
        private Duration interval = Duration.ofSeconds(1);
        /** Extra time after a lease deadline before it is reclaimed (absorbs pauses and clock skew). */
        private Duration leaseGrace = Duration.ofSeconds(2);
        /** Age after which a PENDING job counts as abandoned by its submitter. */
        private Duration pendingGrace = Duration.ofSeconds(5);
        /** Age after which a QUEUED job that nobody picked up is dispatched again. */
        private Duration redispatchAfter = Duration.ofSeconds(60);
        private int batchSize = 500;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public Duration getInterval() { return interval; }
        public void setInterval(Duration interval) { this.interval = interval; }
        public Duration getLeaseGrace() { return leaseGrace; }
        public void setLeaseGrace(Duration leaseGrace) { this.leaseGrace = leaseGrace; }
        public Duration getPendingGrace() { return pendingGrace; }
        public void setPendingGrace(Duration pendingGrace) { this.pendingGrace = pendingGrace; }
        public Duration getRedispatchAfter() { return redispatchAfter; }
        public void setRedispatchAfter(Duration redispatchAfter) { this.redispatchAfter = redispatchAfter; }
        public int getBatchSize() { return batchSize; }
        public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
    }

    public static class Workers {
        private boolean enabled = true;
        private Duration pollTimeout = Duration.ofSeconds(1);
        private Duration shutdownGrace = Duration.ofSeconds(10);
        private List<Pool> pools = new ArrayList<>(List.of(new Pool("default", "default", 4)));

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public Duration getPollTimeout() { return pollTimeout; }
        public void setPollTimeout(Duration pollTimeout) { this.pollTimeout = pollTimeout; }
        public Duration getShutdownGrace() { return shutdownGrace; }
        public void setShutdownGrace(Duration shutdownGrace) { this.shutdownGrace = shutdownGrace; }
        public List<Pool> getPools() { return pools; }
        public void setPools(List<Pool> pools) { this.pools = pools; }
    }

    public static class Pool {
        private String name;
        private String queue;
        private int concurrency = 4;

        public Pool() { }

        public Pool(String name, String queue, int concurrency) {
            this.name = name;
            this.queue = queue;
            this.concurrency = concurrency;
        }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getQueue() { return queue; }
        public void setQueue(String queue) { this.queue = queue; }
        public int getConcurrency() { return concurrency; }
        public void setConcurrency(int concurrency) { this.concurrency = concurrency; }
    }

    public static class Api {
        private Duration defaultTimeout = Duration.ofSeconds(30);
        private Duration maxTimeout = Duration.ofHours(1);
        private int maxAttemptsLimit = 20;
        private int maxPayloadBytes = 64 * 1024;
        /** Reject submissions to queues that no local worker pool serves. */
        private boolean requireServedQueue = true;

        public Duration getDefaultTimeout() { return defaultTimeout; }
        public void setDefaultTimeout(Duration defaultTimeout) { this.defaultTimeout = defaultTimeout; }
        public Duration getMaxTimeout() { return maxTimeout; }
        public void setMaxTimeout(Duration maxTimeout) { this.maxTimeout = maxTimeout; }
        public int getMaxAttemptsLimit() { return maxAttemptsLimit; }
        public void setMaxAttemptsLimit(int maxAttemptsLimit) { this.maxAttemptsLimit = maxAttemptsLimit; }
        public int getMaxPayloadBytes() { return maxPayloadBytes; }
        public void setMaxPayloadBytes(int maxPayloadBytes) { this.maxPayloadBytes = maxPayloadBytes; }
        public boolean isRequireServedQueue() { return requireServedQueue; }
        public void setRequireServedQueue(boolean requireServedQueue) { this.requireServedQueue = requireServedQueue; }
    }

    public static class Dispatcher {
        /** {@code in-memory} (default) or {@code rabbitmq}. */
        private String type = "in-memory";

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
    }

    /** RabbitMQ connection for {@code taskplatform.dispatcher.type=rabbitmq}. */
    public static class Rabbitmq {
        private String host = "127.0.0.1";
        private int port = 5672;
        private String username = "guest";
        private String password = "guest";
        private String virtualHost = "/";
        /** Prefix of the durable RabbitMQ queue that backs each job queue. */
        private String queuePrefix = "taskplatform.";
        /** Unacknowledged messages a consumer may buffer per queue. */
        private int prefetch = 16;

        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        public String getVirtualHost() { return virtualHost; }
        public void setVirtualHost(String virtualHost) { this.virtualHost = virtualHost; }
        public String getQueuePrefix() { return queuePrefix; }
        public void setQueuePrefix(String queuePrefix) { this.queuePrefix = queuePrefix; }
        public int getPrefetch() { return prefetch; }
        public void setPrefetch(int prefetch) { this.prefetch = prefetch; }
    }

    /** Optional Redis: idempotency cache and rate-limit counters only, never authoritative. */
    public static class Redis {
        private boolean enabled = false;
        private String uri = "redis://127.0.0.1:6379";
        /** Command timeout; on timeout or error the caller falls back (fail open). */
        private Duration timeout = Duration.ofMillis(500);
        private String keyPrefix = "taskplatform:";
        /** How long an idempotency answer stays cached. PostgreSQL keeps the key forever. */
        private Duration idempotencyTtl = Duration.ofHours(24);

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public String getUri() { return uri; }
        public void setUri(String uri) { this.uri = uri; }
        public Duration getTimeout() { return timeout; }
        public void setTimeout(Duration timeout) { this.timeout = timeout; }
        public String getKeyPrefix() { return keyPrefix; }
        public void setKeyPrefix(String keyPrefix) { this.keyPrefix = keyPrefix; }
        public Duration getIdempotencyTtl() { return idempotencyTtl; }
        public void setIdempotencyTtl(Duration idempotencyTtl) { this.idempotencyTtl = idempotencyTtl; }
    }

    /** Fixed-window rate limit on job submission, per client (X-Client-Id header, else remote address). */
    public static class RateLimit {
        private boolean enabled = false;
        private int requestsPerWindow = 100;
        private Duration window = Duration.ofSeconds(1);

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public int getRequestsPerWindow() { return requestsPerWindow; }
        public void setRequestsPerWindow(int requestsPerWindow) { this.requestsPerWindow = requestsPerWindow; }
        public Duration getWindow() { return window; }
        public void setWindow(Duration window) { this.window = window; }
    }
}
