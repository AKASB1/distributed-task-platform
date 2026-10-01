package io.akasb.taskplatform.dispatch;

import io.akasb.taskplatform.domain.Job;

/** The requested operation is not possible in the job's current state (for example cancelling a finished job). */
public final class JobStateConflictException extends RuntimeException {
    private final transient Job job;

    public JobStateConflictException(Job job, String message) {
        super(message);
        this.job = job;
    }

    public Job job() {
        return job;
    }
}
