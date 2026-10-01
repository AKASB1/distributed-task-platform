package io.akasb.taskplatform.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * One delivery (attempt) of a job to a worker, kept separate from the job state.
 *
 * @param queuedAt      start of the queue wait that ended with this delivery (for queue latency)
 * @param leaseDeadline the lease is valid until this instant unless renewed
 */
public record Delivery(
        UUID jobId,
        int attempt,
        String leaseOwner,
        Instant leaseDeadline,
        AckState ackState,
        Instant queuedAt,
        Instant startedAt,
        Instant heartbeatAt,
        Instant finishedAt,
        Integer progress,
        String error) {

    public Delivery {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(leaseOwner, "leaseOwner");
        Objects.requireNonNull(leaseDeadline, "leaseDeadline");
        Objects.requireNonNull(ackState, "ackState");
        Objects.requireNonNull(startedAt, "startedAt");
        if (attempt < 1) throw new IllegalArgumentException("attempt must be >= 1");
    }

    public static Delivery lease(UUID jobId, int attempt, String owner, Instant queuedAt, Instant now,
                                 Instant leaseDeadline) {
        return new Delivery(jobId, attempt, owner, leaseDeadline, AckState.LEASED, queuedAt, now, now, null, null,
                null);
    }

    public Delivery renewed(Instant newDeadline, Instant now, Integer newProgress) {
        requireOpen();
        return new Delivery(jobId, attempt, leaseOwner, newDeadline, ackState, queuedAt, startedAt, now, finishedAt,
                newProgress != null ? newProgress : progress, error);
    }

    public Delivery close(AckState outcome, String closeError, Instant now) {
        requireOpen();
        if (!ackState.canTransitionTo(outcome)) {
            throw new IllegalArgumentException("cannot close delivery " + jobId + "#" + attempt + " as " + outcome);
        }
        Integer finalProgress = outcome == AckState.ACKED ? Integer.valueOf(100) : progress;
        return new Delivery(jobId, attempt, leaseOwner, leaseDeadline, outcome, queuedAt, startedAt, heartbeatAt, now,
                finalProgress, closeError);
    }

    public boolean isExpired(Instant now) {
        return ackState.isOpen() && !now.isBefore(leaseDeadline);
    }

    private void requireOpen() {
        if (!ackState.isOpen()) {
            throw new IllegalStateException("delivery " + jobId + "#" + attempt + " is " + ackState);
        }
    }
}
