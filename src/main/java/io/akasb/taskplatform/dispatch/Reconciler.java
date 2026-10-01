package io.akasb.taskplatform.dispatch;

import io.akasb.taskplatform.domain.Delivery;
import io.akasb.taskplatform.domain.Job;
import io.akasb.taskplatform.persistence.JobRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Brings jobs out of limbo. Each pass:
 * <ol>
 *   <li>reclaims leases that expired more than {@code leaseGrace} ago (worker died or stalled);</li>
 *   <li>moves RETRY_WAIT jobs whose backoff elapsed back to QUEUED;</li>
 *   <li>enqueues PENDING jobs older than {@code pendingGrace} (crash between persist and enqueue);</li>
 *   <li>re-dispatches QUEUED jobs not dispatched for {@code redispatchAfter} (lost message).</li>
 * </ol>
 * Every step is a compare-and-set, so concurrent reconcilers (several instances) are safe.
 */
public final class Reconciler {
    private static final Logger log = LoggerFactory.getLogger(Reconciler.class);

    public record Settings(Duration leaseGrace, Duration pendingGrace, Duration redispatchAfter, int batchSize) {
        public Settings {
            Objects.requireNonNull(leaseGrace);
            Objects.requireNonNull(pendingGrace);
            Objects.requireNonNull(redispatchAfter);
            if (batchSize < 1) throw new IllegalArgumentException("batchSize must be >= 1");
        }
    }

    public record Report(int leasesExpired, int retriesReleased, int pendingEnqueued, int redispatched) {
        public int total() {
            return leasesExpired + retriesReleased + pendingEnqueued + redispatched;
        }
    }

    private final JobRepository repository;
    private final JobLifecycle lifecycle;
    private final JobDispatcher dispatcher;
    private final Settings settings;

    public Reconciler(JobRepository repository, JobLifecycle lifecycle, JobDispatcher dispatcher, Settings settings) {
        this.repository = Objects.requireNonNull(repository);
        this.lifecycle = Objects.requireNonNull(lifecycle);
        this.dispatcher = Objects.requireNonNull(dispatcher);
        this.settings = Objects.requireNonNull(settings);
    }

    public Report reconcileOnce() {
        return pass(lifecycle.now().minus(settings.redispatchAfter()), false);
    }

    /**
     * Run once at startup. A non-durable queue lost every message when the process stopped, so every QUEUED job is
     * re-dispatched regardless of when it was last dispatched, in as many batches as needed.
     */
    public Report recoverOnStartup() {
        Instant now = lifecycle.now();
        Report report = pass(dispatcher.durable() ? now.minus(settings.redispatchAfter()) : now.plusNanos(1_000), true);
        if (report.total() > 0) log.info("startup recovery: {}", report);
        return report;
    }

    private Report pass(Instant redispatchBefore, boolean drainRedispatch) {
        Instant now = lifecycle.now();
        Instant expiredBefore = now.minus(settings.leaseGrace());
        int expired = 0;
        for (Delivery d : repository.findExpiredLeases(expiredBefore, settings.batchSize())) {
            if (safely(d, x -> lifecycle.expireLease(x, expiredBefore))) expired++;
        }
        int released = 0;
        for (Job j : repository.findDueRetries(now, settings.batchSize())) {
            if (safely(j, lifecycle::releaseRetry)) released++;
        }
        int pending = 0;
        for (Job j : repository.findStalePending(now.minus(settings.pendingGrace()), settings.batchSize())) {
            if (safely(j, lifecycle::enqueuePending)) pending++;
        }
        int redispatched = 0;
        Set<UUID> seen = new HashSet<>();
        List<Job> batch = repository.findQueuedDispatchedBefore(redispatchBefore, settings.batchSize());
        while (!batch.isEmpty()) {
            boolean progress = false;
            for (Job j : batch) {
                if (seen.add(j.id())) {
                    progress = true;
                    if (safelyRun(j, lifecycle::redispatch)) redispatched++;
                }
            }
            // a periodic pass handles one batch; startup keeps going until every lost QUEUED job was re-sent
            if (!drainRedispatch || !progress) break;
            batch = repository.findQueuedDispatchedBefore(redispatchBefore, settings.batchSize());
        }
        Report report = new Report(expired, released, pending, redispatched);
        if (report.leasesExpired() + report.pendingEnqueued() + report.redispatched() > 0) {
            log.info("reconciler pass: {}", report);
        }
        return report;
    }

    private static <T> boolean safely(T item, java.util.function.Predicate<T> action) {
        try {
            return action.test(item);
        } catch (RuntimeException e) {
            log.error("reconciler step failed for {}", item, e);
            return false;
        }
    }

    private static <T> boolean safelyRun(T item, Consumer<T> action) {
        return safely(item, x -> {
            action.accept(x);
            return true;
        });
    }
}
