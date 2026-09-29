package io.akasb.taskplatform.worker;

import java.time.Instant;
import java.util.Objects;

public final class WorkerLease {
    private final String workerId;
    private final Instant expiresAt;

    public WorkerLease(String workerId, Instant expiresAt) {
        this.workerId = Objects.requireNonNull(workerId);
        this.expiresAt = Objects.requireNonNull(expiresAt);
    }

    public String workerId() { return workerId; }
    public boolean isExpired(Instant now) { return !now.isBefore(expiresAt); }
}
