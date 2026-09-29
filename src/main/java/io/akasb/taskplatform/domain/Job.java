package io.akasb.taskplatform.domain;

import java.util.Objects;

public final class Job {
    private final String id;
    private JobState state = JobState.PENDING;
    private long version = 0;

    public Job(String id) {
        this.id = Objects.requireNonNull(id);
        if (id.isEmpty()) throw new IllegalArgumentException("id required");
    }

    public String id() { return id; }
    public JobState state() { return state; }
    public long version() { return version; }

    public void transition(JobState next, long expectedVersion) {
        if (version != expectedVersion) throw new IllegalStateException("stale version");
        if (!state.canTransitionTo(next)) throw new IllegalStateException("invalid transition");
        state = next;
        version++;
    }
}
