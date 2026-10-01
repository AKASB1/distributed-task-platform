package io.akasb.taskplatform.domain;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable snapshot of a job row. Every state change returns a new snapshot with {@code version + 1};
 * the repository stores it with a compare-and-set on the previous version.
 *
 * @param readyAt       when the job last became eligible to run (start of the current queue wait)
 * @param dispatchedAt  when the job id was last handed to the dispatcher
 * @param nextAttemptAt when a job in {@link JobState#RETRY_WAIT} becomes eligible again
 */
public record Job(
        UUID id,
        String queue,
        String type,
        String payload,
        JobState state,
        long version,
        int attempts,
        int maxAttempts,
        Duration timeout,
        String idempotencyKey,
        Instant createdAt,
        Instant updatedAt,
        Instant readyAt,
        Instant dispatchedAt,
        Instant nextAttemptAt,
        Instant finishedAt,
        String lastError,
        String result) {

    public Job {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");
        if (queue.isBlank()) throw new IllegalArgumentException("queue required");
        if (type.isBlank()) throw new IllegalArgumentException("type required");
        if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be >= 1");
        if (attempts < 0) throw new IllegalArgumentException("attempts must be >= 0");
        if (timeout.isNegative() || timeout.isZero()) throw new IllegalArgumentException("timeout must be positive");
    }

    /** A freshly submitted job, version 0, state PENDING. */
    public static Job pending(UUID id, String queue, String type, String payload, int maxAttempts,
                              Duration timeout, String idempotencyKey, Instant now) {
        return new Job(id, queue, type, payload, JobState.PENDING, 0, 0, maxAttempts, timeout, idempotencyKey,
                now, now, null, null, null, null, null, null);
    }

    /** PENDING or RETRY_WAIT to QUEUED; the queue wait starts at {@code readyAt}. */
    public Job enqueue(Instant readyAt, Instant now) {
        return next(JobState.QUEUED, now, attempts, readyAt, now, null, null, lastError, result);
    }

    /** QUEUED to RUNNING: a new attempt starts. */
    public Job start(Instant now) {
        return next(JobState.RUNNING, now, attempts + 1, readyAt, dispatchedAt, null, null, lastError, result);
    }

    public Job succeed(String resultJson, Instant now) {
        return next(JobState.SUCCEEDED, now, attempts, readyAt, dispatchedAt, null, now, null, resultJson);
    }

    public Job retryAt(Instant nextAttempt, String error, Instant now) {
        return next(JobState.RETRY_WAIT, now, attempts, readyAt, dispatchedAt, nextAttempt, null, error, result);
    }

    public Job fail(String error, Instant now) {
        return next(JobState.FAILED, now, attempts, readyAt, dispatchedAt, null, now, error, result);
    }

    public Job deadLetter(String error, Instant now) {
        return next(JobState.DEAD_LETTER, now, attempts, readyAt, dispatchedAt, null, now, error, result);
    }

    public Job cancel(Instant now) {
        return next(JobState.CANCELLED, now, attempts, readyAt, dispatchedAt, null, now, lastError, result);
    }

    public boolean hasAttemptsLeft() {
        return attempts < maxAttempts;
    }

    private Job next(JobState target, Instant now, int newAttempts, Instant newReadyAt, Instant newDispatchedAt,
                     Instant newNextAttemptAt, Instant newFinishedAt, String newLastError, String newResult) {
        if (!state.canTransitionTo(target)) throw new IllegalTransitionException(state, target);
        return new Job(id, queue, type, payload, target, version + 1, newAttempts, maxAttempts, timeout,
                idempotencyKey, createdAt, now, newReadyAt, newDispatchedAt, newNextAttemptAt, newFinishedAt,
                newLastError, newResult);
    }
}
