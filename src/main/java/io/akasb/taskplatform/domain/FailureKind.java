package io.akasb.taskplatform.domain;

/** Classification of an attempt failure; decides between retry, FAILED, and DEAD_LETTER. */
public enum FailureKind {
    /** Transient error (the default for unexpected exceptions). */
    RETRYABLE(true),
    /** Permanent error: retrying cannot help. The job goes to FAILED. */
    NON_RETRYABLE(false),
    /** The attempt exceeded the job's execution timeout. */
    TIMEOUT(true),
    /** The worker stopped renewing its lease (crash, partition, long pause). */
    LEASE_EXPIRED(true),
    /** The worker was shut down while the attempt was running. */
    WORKER_SHUTDOWN(true);

    private final boolean retryable;

    FailureKind(boolean retryable) {
        this.retryable = retryable;
    }

    public boolean retryable() {
        return retryable;
    }
}
