package io.akasb.taskplatform.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** A lease pairs a RUNNING job snapshot with the delivery of the same attempt. */
class LeaseTest {
    private static final UUID ID = new UUID(3, 4);
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final Instant T1 = T0.plusSeconds(1);
    private static final Instant DEADLINE = T1.plusSeconds(30);

    private static Job queued() {
        return Job.pending(ID, "default", "sleep", "{}", 3, Duration.ofSeconds(10), null, T0).enqueue(T0, T0);
    }

    private static Job running() {
        return queued().start(T1);
    }

    private static Delivery delivery(int attempt) {
        return Delivery.lease(ID, attempt, "worker-a", T0, T1, DEADLINE);
    }

    @Test
    void exposesAttemptOwnerAndDeadlineOfTheDelivery() {
        Job job = running();
        Delivery d = delivery(1);
        Lease lease = new Lease(job, d);

        assertThat(lease.job()).isSameAs(job);
        assertThat(lease.delivery()).isSameAs(d);
        assertThat(lease.attempt()).isEqualTo(1);
        assertThat(lease.owner()).isEqualTo("worker-a");
        assertThat(lease.deadline()).isEqualTo(DEADLINE);
    }

    @Test
    void secondAttemptLeaseMatchesJobAttempts() {
        Job second = running().retryAt(T1.plusSeconds(5), "e", T1).enqueue(T1.plusSeconds(5), T1.plusSeconds(5))
                .start(T1.plusSeconds(6));
        Lease lease = new Lease(second, delivery(2));
        assertThat(lease.attempt()).isEqualTo(2);
    }

    @Test
    void rejectsNullComponents() {
        assertThatThrownBy(() -> new Lease(null, delivery(1)))
                .isInstanceOf(NullPointerException.class).hasMessage("job");
        assertThatThrownBy(() -> new Lease(running(), null))
                .isInstanceOf(NullPointerException.class).hasMessage("delivery");
    }

    @ParameterizedTest
    @EnumSource(value = JobState.class, names = "RUNNING", mode = EnumSource.Mode.EXCLUDE)
    void requiresARunningJob(JobState state) {
        Job job = switch (state) {
            case PENDING -> Job.pending(ID, "q", "t", "{}", 3, Duration.ofSeconds(10), null, T0);
            case QUEUED -> queued();
            case RETRY_WAIT -> running().retryAt(T1.plusSeconds(5), "e", T1);
            case SUCCEEDED -> running().succeed("{}", T1);
            case FAILED -> running().fail("e", T1);
            case DEAD_LETTER -> running().deadLetter("e", T1);
            case CANCELLED -> running().cancel(T1);
            case RUNNING -> throw new AssertionError("excluded");
        };
        assertThat(job.state()).isEqualTo(state);
        assertThatThrownBy(() -> new Lease(job, delivery(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("lease requires a RUNNING job");
    }

    @Test
    void rejectsAttemptMismatchInEitherDirection() {
        assertThatThrownBy(() -> new Lease(running(), delivery(2)))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("attempt mismatch");

        Job second = running().retryAt(T1, "e", T1).enqueue(T1, T1).start(T1);
        assertThatThrownBy(() -> new Lease(second, delivery(1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("attempt mismatch");
    }

    @Test
    void withDeliveryKeepsTheJobSnapshotAsFencingToken() {
        Lease lease = new Lease(running(), delivery(1));
        Delivery renewed = lease.delivery().renewed(DEADLINE.plusSeconds(30), T1.plusSeconds(10), 50);

        Lease next = lease.withDelivery(renewed);

        assertThat(next).isNotSameAs(lease);
        assertThat(next.job()).isSameAs(lease.job());
        assertThat(next.job().version()).isEqualTo(lease.job().version());
        assertThat(next.deadline()).isEqualTo(DEADLINE.plusSeconds(30));
        assertThat(next.delivery().progress()).isEqualTo(50);
        assertThat(lease.deadline()).as("the original lease is unchanged").isEqualTo(DEADLINE);
    }

    @Test
    void withDeliveryRevalidatesTheInvariants() {
        Lease lease = new Lease(running(), delivery(1));
        assertThatThrownBy(() -> lease.withDelivery(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> lease.withDelivery(delivery(2)))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("attempt mismatch");
    }
}
