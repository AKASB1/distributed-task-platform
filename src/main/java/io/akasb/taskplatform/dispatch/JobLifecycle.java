package io.akasb.taskplatform.dispatch;

import io.akasb.taskplatform.domain.AckState;
import io.akasb.taskplatform.domain.Delivery;
import io.akasb.taskplatform.domain.FailureKind;
import io.akasb.taskplatform.domain.Job;
import io.akasb.taskplatform.domain.JobState;
import io.akasb.taskplatform.domain.Lease;
import io.akasb.taskplatform.domain.RetryPolicy;
import io.akasb.taskplatform.domain.RetryPolicy.RetryDecision;
import io.akasb.taskplatform.domain.TaskFailure;
import io.akasb.taskplatform.observability.JobMdc;
import io.akasb.taskplatform.observability.LifecycleListener;
import io.akasb.taskplatform.observability.LifecycleListener.Recovery;
import io.akasb.taskplatform.persistence.InsertResult;
import io.akasb.taskplatform.persistence.JobRepository;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * All job state changes. Each operation reads a snapshot, computes the next snapshot with the domain rules, and
 * writes it with a compare-and-set; a lost race is reported to the caller instead of being overwritten.
 */
public final class JobLifecycle {
    private static final Logger log = LoggerFactory.getLogger(JobLifecycle.class);
    private static final int CANCEL_ATTEMPTS = 10;

    private final JobRepository repository;
    private final JobDispatcher dispatcher;
    private final RetryPolicy retryPolicy;
    private final Clock clock;
    private final Duration leaseDuration;
    private final LifecycleListener listener;
    private final Checkpoints checkpoints;

    public JobLifecycle(JobRepository repository, JobDispatcher dispatcher, RetryPolicy retryPolicy, Clock clock,
                        Duration leaseDuration, LifecycleListener listener, Checkpoints checkpoints) {
        this.repository = Objects.requireNonNull(repository);
        this.dispatcher = Objects.requireNonNull(dispatcher);
        this.retryPolicy = Objects.requireNonNull(retryPolicy);
        this.clock = Objects.requireNonNull(clock);
        this.leaseDuration = Objects.requireNonNull(leaseDuration);
        this.listener = Objects.requireNonNull(listener);
        this.checkpoints = Objects.requireNonNull(checkpoints);
        if (leaseDuration.isNegative() || leaseDuration.isZero()) throw new IllegalArgumentException("leaseDuration");
    }

    /** Current time, truncated to the database precision (microseconds) so both stores see the same values. */
    public Instant now() {
        return clock.instant().truncatedTo(ChronoUnit.MICROS);
    }

    public Duration leaseDuration() {
        return leaseDuration;
    }

    // ---------------------------------------------------------------- submission

    /**
     * Persist (PENDING), move to QUEUED, dispatch. A crash between the steps leaves a PENDING job or a QUEUED job
     * that was never dispatched; the {@link Reconciler} finishes both.
     */
    public SubmitResult submit(SubmitCommand command) {
        Instant now = now();
        Job job = Job.pending(UUID.randomUUID(), command.queue(), command.type(), command.payload(),
                command.maxAttempts(), command.timeout(), command.idempotencyKey(), now);
        InsertResult inserted = repository.insert(job);
        if (!inserted.created()) return new SubmitResult(inserted.job(), false);
        try (var mdc = JobMdc.of(job.id(), job.queue())) {
            listener.onSubmitted(job);
            checkpoints.reached(Checkpoints.Point.AFTER_PERSIST, job.id());
            Job queued = job.enqueue(now, now);
            if (!repository.update(queued, job.version())) {
                // A concurrent cancel (or the reconciler) moved it first; report what is stored.
                return new SubmitResult(repository.find(job.id()).orElse(job), true);
            }
            checkpoints.reached(Checkpoints.Point.AFTER_ENQUEUE_BEFORE_DISPATCH, job.id());
            dispatch(queued);
            log.info("job submitted type={} maxAttempts={}", job.type(), job.maxAttempts());
            return new SubmitResult(queued, true);
        }
    }

    // ---------------------------------------------------------------- worker side

    /** Leases the job to {@code workerId} if it is still QUEUED. Empty if someone else won or it moved on. */
    public Optional<Lease> claim(UUID jobId, String workerId) {
        Optional<Job> found = repository.find(jobId);
        if (found.isEmpty() || found.get().state() != JobState.QUEUED) return Optional.empty();
        Job job = found.get();
        Instant now = now();
        Job running = job.start(now);
        Delivery delivery = Delivery.lease(jobId, running.attempts(), workerId, job.readyAt(), now,
                now.plus(leaseDuration));
        if (!repository.claim(running, job.version(), delivery)) return Optional.empty();
        listener.onClaimed(running, delivery);
        return Optional.of(new Lease(running, delivery));
    }

    /** Renews the lease and records progress. Tells the worker to stop if the job was cancelled or reclaimed. */
    public HeartbeatResult heartbeat(Lease lease, Integer progress) {
        Instant now = now();
        Instant deadline = now.plus(leaseDuration);
        Integer clamped = progress == null ? null : Math.max(0, Math.min(100, progress));
        if (repository.renewLease(lease.job().id(), lease.attempt(), lease.owner(), lease.job().version(), deadline,
                now, clamped)) {
            return HeartbeatResult.renewed(lease.withDelivery(lease.delivery().renewed(deadline, now, clamped)));
        }
        return isCancelled(lease.job().id()) ? HeartbeatResult.cancelled() : HeartbeatResult.lost();
    }

    public Outcome complete(Lease lease, String resultJson) {
        Instant now = now();
        Job next = lease.job().succeed(resultJson, now);
        Delivery closed = lease.delivery().close(AckState.ACKED, null, now);
        if (!repository.finishAttempt(next, lease.job().version(), closed, null)) return rejection(lease);
        listener.onAttemptFinished(next, closed);
        log.info("job succeeded");
        return Outcome.ACCEPTED;
    }

    public Outcome fail(Lease lease, TaskFailure failure) {
        Instant now = now();
        Job job = lease.job();
        RetryDecision decision = retryPolicy.decide(job.attempts(), job.maxAttempts(), failure.kind());
        Job next = afterFailure(job, decision, failure, now);
        Delivery closed = lease.delivery().close(AckState.NACKED, failure.describe(), now);
        if (!repository.finishAttempt(next, job.version(), closed, null)) return rejection(lease);
        reportFailure(next, closed, decision, failure);
        return Outcome.ACCEPTED;
    }

    /** The worker stopped its handler after learning that the job was cancelled. */
    public void acknowledgeCancel(Lease lease) {
        repository.closeDelivery(lease.delivery().close(AckState.CANCELLED, "job cancelled", now()), null);
    }

    // ---------------------------------------------------------------- API side

    /**
     * Cancels a PENDING, QUEUED, RETRY_WAIT or RUNNING job. A running handler learns about it on its next heartbeat;
     * its result is discarded because the version changed. Cancelling a cancelled job returns it unchanged.
     */
    public Job cancel(UUID id) {
        for (int i = 0; i < CANCEL_ATTEMPTS; i++) {
            Job job = repository.find(id).orElseThrow(() -> new JobNotFoundException(id));
            if (job.state() == JobState.CANCELLED) return job;
            if (job.state().isTerminal()) {
                throw new JobStateConflictException(job, "job " + id + " is already " + job.state());
            }
            Job next = job.cancel(now());
            if (repository.update(next, job.version())) {
                try (var mdc = JobMdc.of(id, job.queue())) {
                    log.info("job cancelled from state {}", job.state());
                }
                listener.onCancelled(next, job.state());
                return next;
            }
        }
        Job current = repository.find(id).orElseThrow(() -> new JobNotFoundException(id));
        throw new JobStateConflictException(current, "job " + id + " is changing concurrently; retry the request");
    }

    // ---------------------------------------------------------------- reconciler side

    /**
     * Reclaims an expired lease. If the attempt still owns the job, the job is retried or dead-lettered; otherwise
     * only the delivery is closed. {@code expiredBefore} guards against a renewal that raced with the scan.
     */
    public boolean expireLease(Delivery expired, Instant expiredBefore) {
        Instant now = now();
        String reason = "lease held by " + expired.leaseOwner() + " expired at " + expired.leaseDeadline();
        Job job = repository.find(expired.jobId()).orElse(null);
        try (var mdc = JobMdc.of(expired.jobId(), expired.attempt(), job == null ? null : job.queue(),
                expired.leaseOwner())) {
            if (job == null || job.state() != JobState.RUNNING || job.attempts() != expired.attempt()) {
                return repository.closeDelivery(expired.close(AckState.EXPIRED, reason, now), expiredBefore);
            }
            TaskFailure failure = new TaskFailure(FailureKind.LEASE_EXPIRED, reason);
            RetryDecision decision = retryPolicy.decide(job.attempts(), job.maxAttempts(), failure.kind());
            Job next = afterFailure(job, decision, failure, now);
            Delivery closed = expired.close(AckState.EXPIRED, failure.describe(), now);
            if (!repository.finishAttempt(next, job.version(), closed, expiredBefore)) return false;
            log.warn("stale lease reclaimed; job now {}", next.state());
            listener.onLeaseExpired(job, expired);
            reportFailure(next, closed, decision, failure);
            return true;
        }
    }

    /** RETRY_WAIT to QUEUED once the backoff has elapsed. */
    public boolean releaseRetry(Job job) {
        Instant now = now();
        Job queued = job.enqueue(job.nextAttemptAt(), now);
        if (!repository.update(queued, job.version())) return false;
        listener.onRecovered(Recovery.RETRY_RELEASED, queued);
        dispatch(queued);
        return true;
    }

    /** PENDING to QUEUED for a job whose submitter died before enqueueing it. */
    public boolean enqueuePending(Job job) {
        Instant now = now();
        Job queued = job.enqueue(job.createdAt(), now);
        if (!repository.update(queued, job.version())) return false;
        try (var mdc = JobMdc.of(job.id(), job.queue())) {
            log.warn("recovered job that was persisted but never enqueued");
        }
        listener.onRecovered(Recovery.PENDING_ENQUEUED, queued);
        dispatch(queued);
        return true;
    }

    /** Hands a QUEUED job to the dispatcher again (its earlier message may have been lost). */
    public void redispatch(Job job) {
        repository.markDispatched(job.id(), now());
        listener.onRecovered(Recovery.QUEUED_REDISPATCHED, job);
        dispatch(job);
    }

    // ---------------------------------------------------------------- helpers

    private Job afterFailure(Job job, RetryDecision decision, TaskFailure failure, Instant now) {
        return switch (decision.action()) {
            case RETRY -> job.retryAt(now.plus(decision.delay()), failure.describe(), now);
            case FAIL -> job.fail(failure.describe(), now);
            case DEAD_LETTER -> job.deadLetter("attempts exhausted (" + job.attempts() + "/" + job.maxAttempts()
                    + "); last error: " + failure.describe(), now);
        };
    }

    private void reportFailure(Job next, Delivery closed, RetryDecision decision, TaskFailure failure) {
        if (decision.action() == RetryDecision.Action.RETRY) {
            listener.onRetryScheduled(next, failure.kind(), decision.delay());
            log.info("attempt failed ({}), retry in {} ms", failure.describe(), decision.delay().toMillis());
        } else {
            log.warn("job {}: {}", next.state(), next.lastError());
        }
        listener.onAttemptFinished(next, closed);
    }

    private Outcome rejection(Lease lease) {
        if (isCancelled(lease.job().id())) {
            // The handler finished before its next heartbeat could tell it about the cancellation: close the
            // attempt as CANCELLED now instead of leaving it to expire (no-op if the attempt was already closed).
            repository.closeDelivery(lease.delivery().close(AckState.CANCELLED, "job cancelled", now()), null);
            log.info("report from {} discarded: job was cancelled", lease.owner());
            return Outcome.CANCELLED;
        }
        log.warn("report from {} discarded: lease lost", lease.owner());
        return Outcome.LEASE_LOST;
    }

    private boolean isCancelled(UUID jobId) {
        return repository.find(jobId).map(j -> j.state() == JobState.CANCELLED).orElse(false);
    }

    private void dispatch(Job job) {
        try {
            dispatcher.dispatch(job.queue(), job.id());
        } catch (RuntimeException e) {
            log.warn("dispatch of job {} failed ({}); the reconciler will re-dispatch it", job.id(), e.toString());
        }
    }
}
