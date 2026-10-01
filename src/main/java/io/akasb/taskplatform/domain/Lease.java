package io.akasb.taskplatform.domain;

import java.time.Instant;
import java.util.Objects;

/**
 * What a worker holds while executing an attempt: the job snapshot written at claim time (its version is the
 * fencing token for the whole attempt) and the open delivery.
 */
public record Lease(Job job, Delivery delivery) {
    public Lease {
        Objects.requireNonNull(job, "job");
        Objects.requireNonNull(delivery, "delivery");
        if (job.state() != JobState.RUNNING) throw new IllegalArgumentException("lease requires a RUNNING job");
        if (job.attempts() != delivery.attempt()) throw new IllegalArgumentException("attempt mismatch");
    }

    public int attempt() { return delivery.attempt(); }
    public String owner() { return delivery.leaseOwner(); }
    public Instant deadline() { return delivery.leaseDeadline(); }

    public Lease withDelivery(Delivery renewed) {
        return new Lease(job, renewed);
    }
}
