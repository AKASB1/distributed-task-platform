package io.akasb.taskplatform.dispatch;

import java.util.UUID;

/**
 * Fault-injection hook between the steps of submission. Production uses {@link #none()}; tests throw from
 * {@link #reached} to simulate a crash at that point.
 */
@FunctionalInterface
public interface Checkpoints {

    enum Point {
        /** The job row exists (PENDING) but was not yet moved to QUEUED. */
        AFTER_PERSIST,
        /** The job is QUEUED in the database but its id was not yet handed to the dispatcher. */
        AFTER_ENQUEUE_BEFORE_DISPATCH
    }

    void reached(Point point, UUID jobId);

    static Checkpoints none() {
        return (point, jobId) -> { };
    }
}
