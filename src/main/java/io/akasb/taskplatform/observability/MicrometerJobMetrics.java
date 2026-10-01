package io.akasb.taskplatform.observability;

import io.akasb.taskplatform.domain.Delivery;
import io.akasb.taskplatform.domain.FailureKind;
import io.akasb.taskplatform.domain.Job;
import io.akasb.taskplatform.domain.JobState;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;

/**
 * Micrometer meters for the job lifecycle. In Prometheus format (dots become underscores):
 * <ul>
 *   <li>{@code taskplatform_jobs_submitted_total{queue}}</li>
 *   <li>{@code taskplatform_job_queue_latency_seconds{queue}} histogram: ready to leased</li>
 *   <li>{@code taskplatform_job_execution_latency_seconds{queue,outcome}} histogram: leased to acked/nacked/expired</li>
 *   <li>{@code taskplatform_job_attempts_total{queue,outcome}}</li>
 *   <li>{@code taskplatform_job_retries_total{queue,reason}}</li>
 *   <li>{@code taskplatform_jobs_finished_total{queue,outcome}}: succeeded, failed, dead_letter, cancelled</li>
 *   <li>{@code taskplatform_leases_expired_total{queue}}: stale leases reclaimed</li>
 *   <li>{@code taskplatform_reconciler_recovered_total{kind}}</li>
 * </ul>
 * Worker utilization gauges are registered per pool by the worker configuration.
 */
public final class MicrometerJobMetrics implements LifecycleListener {
    public static final String SUBMITTED = "taskplatform.jobs.submitted";
    public static final String QUEUE_LATENCY = "taskplatform.job.queue.latency";
    public static final String EXECUTION_LATENCY = "taskplatform.job.execution.latency";
    public static final String ATTEMPTS = "taskplatform.job.attempts";
    public static final String RETRIES = "taskplatform.job.retries";
    public static final String FINISHED = "taskplatform.jobs.finished";
    public static final String LEASES_EXPIRED = "taskplatform.leases.expired";
    public static final String RECOVERED = "taskplatform.reconciler.recovered";

    private final MeterRegistry registry;

    public MicrometerJobMetrics(MeterRegistry registry) {
        this.registry = Objects.requireNonNull(registry);
    }

    /**
     * Registers the per-queue counters and timers at zero so that every series exists in Prometheus before the first
     * event (absent series make rates and alerts awkward).
     */
    public void registerQueue(String queue) {
        Counter.builder(SUBMITTED).description("Jobs accepted by the API").tag("queue", queue).register(registry);
        timer(QUEUE_LATENCY, "Time from ready to leased by a worker", queue, null);
        for (String outcome : new String[] {"acked", "nacked", "expired"}) {
            timer(EXECUTION_LATENCY, "Attempt duration from lease to acknowledgement", queue, outcome);
            Counter.builder(ATTEMPTS).description("Finished attempts by outcome")
                    .tag("queue", queue).tag("outcome", outcome).register(registry);
        }
        for (FailureKind kind : FailureKind.values()) {
            if (kind.retryable()) {
                Counter.builder(RETRIES).description("Retries scheduled")
                        .tag("queue", queue).tag("reason", lower(kind.name())).register(registry);
            }
        }
        for (JobState state : JobState.values()) {
            if (state.isTerminal()) {
                Counter.builder(FINISHED).description("Jobs that reached a terminal state")
                        .tag("queue", queue).tag("outcome", lower(state.name())).register(registry);
            }
        }
        Counter.builder(LEASES_EXPIRED).description("Stale leases reclaimed by the reconciler")
                .tag("queue", queue).register(registry);
        for (Recovery kind : Recovery.values()) {
            Counter.builder(RECOVERED).description("Jobs moved on by the reconciler")
                    .tag("kind", lower(kind.name())).register(registry);
        }
    }

    @Override
    public void onSubmitted(Job job) {
        Counter.builder(SUBMITTED).description("Jobs accepted by the API")
                .tag("queue", job.queue()).register(registry).increment();
    }

    @Override
    public void onClaimed(Job running, Delivery delivery) {
        timer(QUEUE_LATENCY, "Time from ready to leased by a worker", running.queue(), null)
                .record(Latency.nonNegative(delivery.queuedAt(), delivery.startedAt()));
    }

    @Override
    public void onAttemptFinished(Job next, Delivery closed) {
        String outcome = lower(closed.ackState().name());
        timer(EXECUTION_LATENCY, "Attempt duration from lease to acknowledgement", next.queue(), outcome)
                .record(Latency.nonNegative(closed.startedAt(), closed.finishedAt()));
        Counter.builder(ATTEMPTS).description("Finished attempts by outcome")
                .tag("queue", next.queue()).tag("outcome", outcome).register(registry).increment();
        if (next.state().isTerminal()) finished(next);
    }

    @Override
    public void onRetryScheduled(Job next, FailureKind reason, Duration delay) {
        Counter.builder(RETRIES).description("Retries scheduled")
                .tag("queue", next.queue()).tag("reason", lower(reason.name())).register(registry).increment();
    }

    @Override
    public void onLeaseExpired(Job running, Delivery expired) {
        Counter.builder(LEASES_EXPIRED).description("Stale leases reclaimed by the reconciler")
                .tag("queue", running.queue()).register(registry).increment();
    }

    @Override
    public void onCancelled(Job cancelled, JobState from) {
        finished(cancelled);
    }

    @Override
    public void onRecovered(Recovery kind, Job job) {
        Counter.builder(RECOVERED).description("Jobs moved on by the reconciler")
                .tag("kind", lower(kind.name())).register(registry).increment();
    }

    private void finished(Job job) {
        Counter.builder(FINISHED).description("Jobs that reached a terminal state")
                .tag("queue", job.queue()).tag("outcome", lower(job.state().name())).register(registry).increment();
    }

    private Timer timer(String name, String description, String queue, String outcome) {
        Timer.Builder b = Timer.builder(name).description(description).tag("queue", queue)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofMinutes(10));
        if (outcome != null) b = b.tag("outcome", outcome);
        return b.register(registry);
    }

    private static String lower(String s) {
        return s.toLowerCase(Locale.ROOT);
    }
}
