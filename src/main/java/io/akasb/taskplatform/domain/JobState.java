package io.akasb.taskplatform.domain;

public enum JobState {
    PENDING, QUEUED, RUNNING, SUCCEEDED, FAILED, RETRY_WAIT, CANCELLED;

    public boolean canTransitionTo(JobState next) {
        switch (this) {
            case PENDING: return next == QUEUED || next == CANCELLED;
            case QUEUED: return next == RUNNING || next == CANCELLED;
            case RUNNING: return next == SUCCEEDED || next == FAILED || next == CANCELLED;
            case FAILED: return next == RETRY_WAIT;
            case RETRY_WAIT: return next == QUEUED || next == CANCELLED;
            default: return false;
        }
    }
}
