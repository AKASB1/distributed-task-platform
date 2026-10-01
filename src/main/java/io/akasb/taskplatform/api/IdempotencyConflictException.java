package io.akasb.taskplatform.api;

import java.util.UUID;

/** The idempotency key was already used for a different request. Maps to 422. */
public final class IdempotencyConflictException extends RuntimeException {
    private final UUID existingJobId;

    public IdempotencyConflictException(String key, UUID existingJobId) {
        super("Idempotency-Key '" + key + "' was already used for a different request (job " + existingJobId + ")");
        this.existingJobId = existingJobId;
    }

    public UUID existingJobId() {
        return existingJobId;
    }
}
