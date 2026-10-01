package io.akasb.taskplatform.worker;

import io.akasb.taskplatform.dispatch.HeartbeatResult;
import io.akasb.taskplatform.dispatch.Outcome;
import io.akasb.taskplatform.domain.Lease;
import io.akasb.taskplatform.domain.TaskFailure;
import java.time.Duration;
import java.util.Optional;

/**
 * Everything a worker says to the control plane. The in-process implementation is {@link LocalWorkerProtocol}; a
 * remote transport (for example gRPC) would implement the same calls.
 */
public interface WorkerProtocol {

    /** Waits up to {@code wait} for a job on {@code queue} and leases its next attempt to {@code workerId}. */
    Optional<Lease> acquireNext(String workerId, String queue, Duration wait) throws InterruptedException;

    /** Renews the lease (heartbeat) and reports progress in percent (nullable). */
    HeartbeatResult heartbeat(Lease lease, Integer progress);

    Outcome complete(Lease lease, String resultJson);

    Outcome fail(Lease lease, TaskFailure failure);

    /** Confirms that the handler was stopped after a {@link HeartbeatResult.Status#CANCELLED} heartbeat. */
    void acknowledgeCancel(Lease lease);
}
