package io.akasb.taskplatform.domain;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Job lifecycle states and the explicit transition table.
 *
 * <pre>
 * PENDING ──► QUEUED ──► RUNNING ──► SUCCEEDED
 *    │          │  ▲        ├──► RETRY_WAIT ──► QUEUED
 *    │          │  └────────┤        │
 *    │          │           ├──► FAILED       (non-retryable failure)
 *    │          │           ├──► DEAD_LETTER  (attempts exhausted)
 *    └──────────┴───────────┴──► CANCELLED ◄── RETRY_WAIT
 * </pre>
 */
public enum JobState {
    /** Persisted, not yet handed to the dispatcher. The reconciler enqueues stragglers. */
    PENDING,
    /** Eligible for execution and handed to the dispatcher. */
    QUEUED,
    /** A worker holds a lease on the current attempt. */
    RUNNING,
    /** The last attempt failed retryably; waiting for the backoff to elapse. */
    RETRY_WAIT,
    SUCCEEDED,
    /** The last attempt failed with a non-retryable error. */
    FAILED,
    /** Every allowed attempt failed retryably (or timed out, or lost its lease). */
    DEAD_LETTER,
    CANCELLED;

    private static final Map<JobState, Set<JobState>> TRANSITIONS = new EnumMap<>(JobState.class);

    static {
        TRANSITIONS.put(PENDING, EnumSet.of(QUEUED, CANCELLED));
        TRANSITIONS.put(QUEUED, EnumSet.of(RUNNING, CANCELLED));
        TRANSITIONS.put(RUNNING, EnumSet.of(SUCCEEDED, RETRY_WAIT, FAILED, DEAD_LETTER, CANCELLED));
        TRANSITIONS.put(RETRY_WAIT, EnumSet.of(QUEUED, CANCELLED));
        TRANSITIONS.put(SUCCEEDED, EnumSet.noneOf(JobState.class));
        TRANSITIONS.put(FAILED, EnumSet.noneOf(JobState.class));
        TRANSITIONS.put(DEAD_LETTER, EnumSet.noneOf(JobState.class));
        TRANSITIONS.put(CANCELLED, EnumSet.noneOf(JobState.class));
    }

    public boolean canTransitionTo(JobState next) {
        return TRANSITIONS.get(this).contains(next);
    }

    public Set<JobState> allowedNext() {
        return Collections.unmodifiableSet(TRANSITIONS.get(this));
    }

    public boolean isTerminal() {
        return TRANSITIONS.get(this).isEmpty();
    }
}
