package io.akasb.taskplatform.dispatch;

/** Answer to a completion or failure report from a worker. */
public enum Outcome {
    ACCEPTED,
    /** The attempt no longer owns the job (lease expired and was reclaimed); the report was discarded. */
    LEASE_LOST,
    /** The job was cancelled; the report was discarded. */
    CANCELLED
}
