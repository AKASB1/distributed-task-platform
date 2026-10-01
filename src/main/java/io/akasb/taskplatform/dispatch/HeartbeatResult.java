package io.akasb.taskplatform.dispatch;

import io.akasb.taskplatform.domain.Lease;

/** Answer to a lease renewal. {@code lease} carries the extended deadline when {@code status == RENEWED}. */
public record HeartbeatResult(Status status, Lease lease) {
    public enum Status {
        RENEWED,
        /** The job was cancelled: stop the handler and acknowledge the cancellation. */
        CANCELLED,
        /** The lease is gone (expired and reclaimed): stop the handler and report nothing. */
        LOST
    }

    public static HeartbeatResult renewed(Lease lease) { return new HeartbeatResult(Status.RENEWED, lease); }
    public static HeartbeatResult cancelled() { return new HeartbeatResult(Status.CANCELLED, null); }
    public static HeartbeatResult lost() { return new HeartbeatResult(Status.LOST, null); }
}
