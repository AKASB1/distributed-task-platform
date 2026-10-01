package io.akasb.taskplatform.persistence;

import io.akasb.taskplatform.domain.Delivery;
import io.akasb.taskplatform.domain.Job;
import io.akasb.taskplatform.domain.JobState;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Durable job store. Every mutating operation is atomic and guarded by a compare-and-set on the job version
 * ({@code UPDATE ... WHERE id = ? AND version = ?}); a {@code false} result means another writer won and the caller
 * must re-read.
 *
 * <p>Invariant relied on by callers: every change to a job row bumps its version, except
 * {@link #markDispatched}, which only touches the dispatch timestamp of QUEUED jobs. Lease renewals write the
 * delivery row only, so the job version stays stable for the whole attempt and acts as its fencing token.
 */
public interface JobRepository {

    /**
     * Inserts a new job. If its idempotency key is already taken, nothing is inserted and the existing job is
     * returned with {@code created = false}.
     */
    InsertResult insert(Job job);

    Optional<Job> find(UUID id);

    Optional<Job> findByIdempotencyKey(String idempotencyKey);

    /** Writes {@code next} iff the stored version equals {@code expectedVersion}. */
    boolean update(Job next, long expectedVersion);

    /** Atomically: job CAS (QUEUED to RUNNING) and insert of the new open delivery. */
    boolean claim(Job running, long expectedVersion, Delivery delivery);

    /**
     * Atomically: job CAS and closing of the open delivery {@code (jobId, attempt)} owned by
     * {@code closed.leaseOwner()}. If {@code leaseExpiredBefore} is non-null the delivery must also still have a
     * lease deadline before that instant (so a concurrent renewal wins over expiry).
     */
    boolean finishAttempt(Job next, long expectedVersion, Delivery closed, Instant leaseExpiredBefore);

    /**
     * Extends the lease iff the delivery is still open and owned by {@code owner} and the job is still RUNNING at
     * {@code jobVersion} (a cancel or a reclaim bumps the version, so the renewal fails).
     */
    boolean renewLease(UUID jobId, int attempt, String owner, long jobVersion, Instant newDeadline,
                       Instant heartbeatAt, Integer progress);

    /**
     * Closes an open delivery without touching the job (the job is already terminal or was moved on). If
     * {@code leaseExpiredBefore} is non-null the lease deadline must be before that instant.
     */
    boolean closeDelivery(Delivery closed, Instant leaseExpiredBefore);

    /** Records a re-dispatch of a QUEUED job. Does not bump the version. */
    void markDispatched(UUID id, Instant at);

    /** All deliveries of a job, ordered by attempt. */
    List<Delivery> deliveries(UUID jobId);

    /** Open deliveries whose lease deadline is before {@code before}, oldest first. */
    List<Delivery> findExpiredLeases(Instant before, int limit);

    /** RETRY_WAIT jobs whose next attempt is due at or before {@code now}. */
    List<Job> findDueRetries(Instant now, int limit);

    /** PENDING jobs created before {@code before} (persisted but never enqueued). */
    List<Job> findStalePending(Instant before, int limit);

    /** QUEUED jobs last dispatched before {@code before} (possibly lost by a non-durable queue). */
    List<Job> findQueuedDispatchedBefore(Instant before, int limit);

    QueueStats queueStats(String queue, Instant now);

    /** Count of jobs per state across all queues. */
    Map<JobState, Long> countByState();
}
