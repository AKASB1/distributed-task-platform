package io.akasb.taskplatform.worker;

import io.akasb.taskplatform.dispatch.HeartbeatResult;
import io.akasb.taskplatform.dispatch.JobDispatcher;
import io.akasb.taskplatform.dispatch.JobLifecycle;
import io.akasb.taskplatform.dispatch.Outcome;
import io.akasb.taskplatform.domain.Lease;
import io.akasb.taskplatform.domain.TaskFailure;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** In-process worker protocol: pulls ids from the {@link JobDispatcher} and calls the {@link JobLifecycle}. */
public final class LocalWorkerProtocol implements WorkerProtocol {
    private final JobDispatcher dispatcher;
    private final JobLifecycle lifecycle;

    public LocalWorkerProtocol(JobDispatcher dispatcher, JobLifecycle lifecycle) {
        this.dispatcher = Objects.requireNonNull(dispatcher);
        this.lifecycle = Objects.requireNonNull(lifecycle);
    }

    @Override
    public Optional<Lease> acquireNext(String workerId, String queue, Duration wait) throws InterruptedException {
        long deadline = System.nanoTime() + wait.toNanos();
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) return Optional.empty();
            Optional<UUID> id = dispatcher.poll(queue, Duration.ofNanos(remaining));
            if (id.isEmpty()) return Optional.empty();
            Optional<Lease> lease = lifecycle.claim(id.get(), workerId);
            if (lease.isPresent()) return lease;
            // Stale message: the job was cancelled, already claimed, or is not QUEUED any more. Drop it.
        }
    }

    @Override
    public HeartbeatResult heartbeat(Lease lease, Integer progress) {
        return lifecycle.heartbeat(lease, progress);
    }

    @Override
    public Outcome complete(Lease lease, String resultJson) {
        return lifecycle.complete(lease, resultJson);
    }

    @Override
    public Outcome fail(Lease lease, TaskFailure failure) {
        return lifecycle.fail(lease, failure);
    }

    @Override
    public void acknowledgeCancel(Lease lease) {
        lifecycle.acknowledgeCancel(lease);
    }
}
