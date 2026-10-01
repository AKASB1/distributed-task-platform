package io.akasb.taskplatform.observability;

import io.akasb.taskplatform.domain.Delivery;
import io.akasb.taskplatform.domain.FailureKind;
import io.akasb.taskplatform.domain.Job;
import io.akasb.taskplatform.domain.JobState;
import java.time.Duration;
import java.util.List;

/** Events emitted by the job lifecycle after a successful state change. Implementations must not throw. */
public interface LifecycleListener {

    enum Recovery { PENDING_ENQUEUED, QUEUED_REDISPATCHED, RETRY_RELEASED }

    default void onSubmitted(Job job) { }

    /** A worker leased a new attempt; {@code delivery.queuedAt()} to {@code startedAt()} is the queue latency. */
    default void onClaimed(Job running, Delivery delivery) { }

    /** An attempt ended (ACKED, NACKED, EXPIRED); {@code next} is the job after the attempt. */
    default void onAttemptFinished(Job next, Delivery closed) { }

    default void onRetryScheduled(Job next, FailureKind reason, Duration delay) { }

    /** A lease ran out and was reclaimed by the reconciler (a stale lease). */
    default void onLeaseExpired(Job running, Delivery expired) { }

    default void onCancelled(Job cancelled, JobState from) { }

    default void onRecovered(Recovery kind, Job job) { }

    static LifecycleListener none() {
        return new LifecycleListener() { };
    }

    static LifecycleListener composite(List<LifecycleListener> listeners) {
        List<LifecycleListener> all = List.copyOf(listeners);
        return new LifecycleListener() {
            @Override public void onSubmitted(Job job) { all.forEach(l -> l.onSubmitted(job)); }
            @Override public void onClaimed(Job running, Delivery d) { all.forEach(l -> l.onClaimed(running, d)); }
            @Override public void onAttemptFinished(Job next, Delivery d) { all.forEach(l -> l.onAttemptFinished(next, d)); }
            @Override public void onRetryScheduled(Job next, FailureKind r, Duration delay) { all.forEach(l -> l.onRetryScheduled(next, r, delay)); }
            @Override public void onLeaseExpired(Job j, Delivery d) { all.forEach(l -> l.onLeaseExpired(j, d)); }
            @Override public void onCancelled(Job c, JobState from) { all.forEach(l -> l.onCancelled(c, from)); }
            @Override public void onRecovered(Recovery k, Job job) { all.forEach(l -> l.onRecovered(k, job)); }
        };
    }
}
