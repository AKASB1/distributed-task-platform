package io.akasb.taskplatform.dispatch;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Per-queue FIFO in process memory. A job id that is already waiting is not added twice, so re-dispatch by the
 * reconciler cannot grow the queue without bound. Not durable: everything is lost on restart.
 */
public final class InMemoryDispatcher implements JobDispatcher {
    private final ConcurrentMap<String, QueueState> queues = new ConcurrentHashMap<>();

    @Override
    public void dispatch(String queue, UUID jobId) {
        QueueState state = state(queue);
        if (state.waiting.add(jobId)) state.ids.add(jobId);
    }

    @Override
    public Optional<UUID> poll(String queue, Duration timeout) throws InterruptedException {
        QueueState state = state(queue);
        UUID id = state.ids.poll(Math.max(0, timeout.toNanos()), TimeUnit.NANOSECONDS);
        if (id != null) state.waiting.remove(id);
        return Optional.ofNullable(id);
    }

    @Override
    public long depth(String queue) {
        return state(queue).ids.size();
    }

    private QueueState state(String queue) {
        return queues.computeIfAbsent(queue, q -> new QueueState());
    }

    private static final class QueueState {
        final LinkedBlockingQueue<UUID> ids = new LinkedBlockingQueue<>();
        final Set<UUID> waiting = ConcurrentHashMap.newKeySet();
    }
}
