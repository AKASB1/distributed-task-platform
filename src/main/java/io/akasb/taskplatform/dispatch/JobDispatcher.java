package io.akasb.taskplatform.dispatch;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Transport for job ids between the control plane and workers. The job state lives in the database; a message is
 * only a hint that a QUEUED job exists, so delivery may be lossy or duplicated: workers claim with a compare-and-set
 * and the reconciler re-dispatches QUEUED jobs that were never picked up.
 *
 * <p>Implementations: {@link InMemoryDispatcher} (default) and
 * {@link io.akasb.taskplatform.dispatch.rabbitmq.RabbitMqDispatcher} ({@code taskplatform.dispatcher.type=rabbitmq}).
 */
public interface JobDispatcher extends AutoCloseable {

    /** Hands {@code jobId} to the queue {@code queue}. May throw if the transport is unavailable. */
    void dispatch(String queue, UUID jobId);

    /** Waits up to {@code timeout} for the next job id on {@code queue}. */
    Optional<UUID> poll(String queue, Duration timeout) throws InterruptedException;

    /**
     * Whether dispatched messages survive a restart of this process. When false, the reconciler re-dispatches every
     * QUEUED job at startup.
     */
    default boolean durable() {
        return false;
    }

    /** Approximate number of waiting messages, or -1 if unknown. */
    default long depth(String queue) {
        return -1;
    }

    @Override
    default void close() {
    }
}
