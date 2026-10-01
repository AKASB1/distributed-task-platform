package io.akasb.taskplatform.persistence;

import io.akasb.taskplatform.domain.AckState;
import io.akasb.taskplatform.domain.Delivery;
import io.akasb.taskplatform.domain.Job;
import io.akasb.taskplatform.domain.JobState;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Predicate;

/**
 * In-memory implementation with the same compare-and-set semantics as the JDBC one. A single monitor makes every
 * operation atomic. Used by fast unit tests; PostgreSQL is the store used by the running service.
 */
public final class InMemoryJobRepository implements JobRepository {
    private final Map<UUID, Job> jobs = new HashMap<>();
    private final Map<String, UUID> idempotencyKeys = new HashMap<>();
    private final Map<UUID, TreeMap<Integer, Delivery>> deliveries = new HashMap<>();

    @Override
    public synchronized InsertResult insert(Job job) {
        if (jobs.containsKey(job.id())) throw new IllegalArgumentException("duplicate job id " + job.id());
        if (job.idempotencyKey() != null) {
            UUID existing = idempotencyKeys.get(job.idempotencyKey());
            if (existing != null) return new InsertResult(jobs.get(existing), false);
            idempotencyKeys.put(job.idempotencyKey(), job.id());
        }
        jobs.put(job.id(), job);
        return new InsertResult(job, true);
    }

    @Override
    public synchronized Optional<Job> find(UUID id) {
        return Optional.ofNullable(jobs.get(id));
    }

    @Override
    public synchronized Optional<Job> findByIdempotencyKey(String idempotencyKey) {
        UUID id = idempotencyKeys.get(idempotencyKey);
        return id == null ? Optional.empty() : Optional.ofNullable(jobs.get(id));
    }

    @Override
    public synchronized boolean update(Job next, long expectedVersion) {
        Job current = jobs.get(next.id());
        if (current == null || current.version() != expectedVersion) return false;
        jobs.put(next.id(), next);
        return true;
    }

    @Override
    public synchronized boolean claim(Job running, long expectedVersion, Delivery delivery) {
        Job current = jobs.get(running.id());
        if (current == null || current.version() != expectedVersion) return false;
        TreeMap<Integer, Delivery> byAttempt = deliveries.computeIfAbsent(running.id(), k -> new TreeMap<>());
        if (byAttempt.containsKey(delivery.attempt())) return false;
        jobs.put(running.id(), running);
        byAttempt.put(delivery.attempt(), delivery);
        return true;
    }

    @Override
    public synchronized boolean finishAttempt(Job next, long expectedVersion, Delivery closed,
                                              Instant leaseExpiredBefore) {
        Job current = jobs.get(next.id());
        if (current == null || current.version() != expectedVersion) return false;
        if (!canClose(closed, leaseExpiredBefore)) return false;
        jobs.put(next.id(), next);
        deliveries.get(closed.jobId()).put(closed.attempt(), closed);
        return true;
    }

    @Override
    public synchronized boolean renewLease(UUID jobId, int attempt, String owner, long jobVersion,
                                           Instant newDeadline, Instant heartbeatAt, Integer progress) {
        Job job = jobs.get(jobId);
        if (job == null || job.version() != jobVersion || job.state() != JobState.RUNNING) return false;
        Delivery open = openDelivery(jobId, attempt, owner);
        if (open == null) return false;
        deliveries.get(jobId).put(attempt, open.renewed(newDeadline, heartbeatAt, progress));
        return true;
    }

    @Override
    public synchronized boolean closeDelivery(Delivery closed, Instant leaseExpiredBefore) {
        if (!canClose(closed, leaseExpiredBefore)) return false;
        deliveries.get(closed.jobId()).put(closed.attempt(), closed);
        return true;
    }

    @Override
    public synchronized void markDispatched(UUID id, Instant at) {
        Job job = jobs.get(id);
        if (job != null && job.state() == JobState.QUEUED) {
            jobs.put(id, new Job(job.id(), job.queue(), job.type(), job.payload(), job.state(), job.version(),
                    job.attempts(), job.maxAttempts(), job.timeout(), job.idempotencyKey(), job.createdAt(),
                    job.updatedAt(), job.readyAt(), at, job.nextAttemptAt(), job.finishedAt(), job.lastError(),
                    job.result()));
        }
    }

    @Override
    public synchronized List<Delivery> deliveries(UUID jobId) {
        TreeMap<Integer, Delivery> byAttempt = deliveries.get(jobId);
        return byAttempt == null ? List.of() : List.copyOf(byAttempt.values());
    }

    @Override
    public synchronized List<Delivery> findExpiredLeases(Instant before, int limit) {
        return deliveries.values().stream()
                .flatMap(m -> m.values().stream())
                .filter(d -> d.ackState() == AckState.LEASED && d.leaseDeadline().isBefore(before))
                .sorted(Comparator.comparing(Delivery::leaseDeadline))
                .limit(limit)
                .toList();
    }

    @Override
    public synchronized List<Job> findDueRetries(Instant now, int limit) {
        return select(j -> j.state() == JobState.RETRY_WAIT && !j.nextAttemptAt().isAfter(now),
                Comparator.comparing(Job::nextAttemptAt), limit);
    }

    @Override
    public synchronized List<Job> findStalePending(Instant before, int limit) {
        return select(j -> j.state() == JobState.PENDING && j.createdAt().isBefore(before),
                Comparator.comparing(Job::createdAt), limit);
    }

    @Override
    public synchronized List<Job> findQueuedDispatchedBefore(Instant before, int limit) {
        return select(j -> j.state() == JobState.QUEUED
                        && (j.dispatchedAt() == null || j.dispatchedAt().isBefore(before)),
                Comparator.comparing(j -> j.dispatchedAt() == null ? Instant.MIN : j.dispatchedAt()), limit);
    }

    @Override
    public synchronized QueueStats queueStats(String queue, Instant now) {
        Map<JobState, Long> counts = new EnumMap<>(JobState.class);
        Instant oldest = null;
        for (Job j : jobs.values()) {
            if (!j.queue().equals(queue)) continue;
            counts.merge(j.state(), 1L, Long::sum);
            if (j.state() == JobState.QUEUED && j.readyAt() != null
                    && (oldest == null || j.readyAt().isBefore(oldest))) {
                oldest = j.readyAt();
            }
        }
        return new QueueStats(queue, counts, oldest, now);
    }

    @Override
    public synchronized Map<JobState, Long> countByState() {
        Map<JobState, Long> counts = new EnumMap<>(JobState.class);
        for (Job j : jobs.values()) counts.merge(j.state(), 1L, Long::sum);
        return counts;
    }

    private boolean canClose(Delivery closed, Instant leaseExpiredBefore) {
        Delivery open = openDelivery(closed.jobId(), closed.attempt(), closed.leaseOwner());
        if (open == null) return false;
        return leaseExpiredBefore == null || open.leaseDeadline().isBefore(leaseExpiredBefore);
    }

    private Delivery openDelivery(UUID jobId, int attempt, String owner) {
        TreeMap<Integer, Delivery> byAttempt = deliveries.get(jobId);
        if (byAttempt == null) return null;
        Delivery d = byAttempt.get(attempt);
        if (d == null || d.ackState() != AckState.LEASED || !d.leaseOwner().equals(owner)) return null;
        return d;
    }

    private List<Job> select(Predicate<Job> filter, Comparator<Job> order, int limit) {
        List<Job> result = new ArrayList<>();
        jobs.values().stream().filter(filter).sorted(order).limit(limit).forEach(result::add);
        return result;
    }
}
