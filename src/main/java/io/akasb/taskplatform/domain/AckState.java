package io.akasb.taskplatform.domain;

/** Acknowledgement state of one delivery (attempt). Only LEASED is open; every other state is final. */
public enum AckState {
    /** A worker holds the lease and is executing the attempt. */
    LEASED,
    /** The worker reported success. */
    ACKED,
    /** The worker reported a failure (including an execution timeout). */
    NACKED,
    /** The lease ran out without an acknowledgement; the reconciler reclaimed the attempt. */
    EXPIRED,
    /** The job was cancelled while this attempt was running. */
    CANCELLED;

    public boolean isOpen() {
        return this == LEASED;
    }

    public boolean canTransitionTo(AckState next) {
        return this == LEASED && next != LEASED;
    }
}
