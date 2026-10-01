package io.akasb.taskplatform.api;

import java.util.Optional;
import java.util.UUID;

/**
 * Optional cache in front of the idempotency index: idempotency key to job id. It is never authoritative. A hit is
 * only a hint that {@link JobService} verifies against the database; a miss, a stale entry or an error always falls
 * through to the database, so the cache can neither create, hide nor change a job.
 *
 * <p>Implementations should not throw (an unavailable cache is a miss); {@link JobService} tolerates it if they do.
 */
public interface IdempotencyCache {

    /** The job id last remembered for {@code key}, or empty on a miss or when the cache is unavailable. */
    Optional<UUID> find(String key);

    /** Records that {@code key} belongs to {@code jobId}. Best effort. */
    void remember(String key, UUID jobId);

    /** No cache: every lookup misses, nothing is stored. */
    static IdempotencyCache none() {
        return NoIdempotencyCache.INSTANCE;
    }
}

/** The {@link IdempotencyCache#none()} implementation. */
enum NoIdempotencyCache implements IdempotencyCache {
    INSTANCE;

    @Override
    public Optional<UUID> find(String key) {
        return Optional.empty();
    }

    @Override
    public void remember(String key, UUID jobId) {
        // nothing to remember
    }

    @Override
    public String toString() {
        return "IdempotencyCache.none()";
    }
}
