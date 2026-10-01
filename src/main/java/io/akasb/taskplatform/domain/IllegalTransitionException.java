package io.akasb.taskplatform.domain;

public final class IllegalTransitionException extends IllegalStateException {
    private final JobState from;
    private final JobState to;

    public IllegalTransitionException(JobState from, JobState to) {
        super("invalid transition " + from + " -> " + to);
        this.from = from;
        this.to = to;
    }

    public JobState from() { return from; }
    public JobState to() { return to; }
}
