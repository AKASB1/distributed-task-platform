package io.akasb.taskplatform.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/** One delivery (attempt): leasing, renewal, closing to each final state, and the lease-expiry boundary. */
class DeliveryTest {
    private static final UUID JOB = new UUID(7, 7);
    private static final Instant QUEUED_AT = Instant.parse("2026-01-01T00:00:00Z");
    private static final Instant T0 = QUEUED_AT.plusSeconds(2);
    private static final Instant DEADLINE = T0.plusSeconds(30);

    private static Delivery leased() {
        return Delivery.lease(JOB, 1, "worker-a", QUEUED_AT, T0, DEADLINE);
    }

    // ---------------------------------------------------------------------------------- lease

    @Test
    void leaseStartsOpenWithHeartbeatAtStartAndNoOutcome() {
        Delivery d = leased();

        assertThat(d.jobId()).isEqualTo(JOB);
        assertThat(d.attempt()).isEqualTo(1);
        assertThat(d.leaseOwner()).isEqualTo("worker-a");
        assertThat(d.leaseDeadline()).isEqualTo(DEADLINE);
        assertThat(d.ackState()).isEqualTo(AckState.LEASED);
        assertThat(d.queuedAt()).isEqualTo(QUEUED_AT);
        assertThat(d.startedAt()).isEqualTo(T0);
        assertThat(d.heartbeatAt()).isEqualTo(T0);
        assertThat(d.finishedAt()).isNull();
        assertThat(d.progress()).isNull();
        assertThat(d.error()).isNull();
    }

    @Test
    void leaseAcceptsAMissingQueuedAt() {
        assertThat(Delivery.lease(JOB, 3, "w", null, T0, DEADLINE).queuedAt()).isNull();
    }

    // -------------------------------------------------------------------------------- renewed

    @Test
    void renewedMovesDeadlineAndHeartbeatAndRecordsProgress() {
        Delivery original = leased();
        Instant hb = T0.plusSeconds(10);
        Delivery renewed = original.renewed(hb.plusSeconds(30), hb, 40);

        assertThat(renewed.leaseDeadline()).isEqualTo(hb.plusSeconds(30));
        assertThat(renewed.heartbeatAt()).isEqualTo(hb);
        assertThat(renewed.progress()).isEqualTo(40);
        assertThat(renewed.ackState()).isEqualTo(AckState.LEASED);
        assertThat(renewed.startedAt()).isEqualTo(T0);
        assertThat(renewed.queuedAt()).isEqualTo(QUEUED_AT);
        assertThat(renewed.leaseOwner()).isEqualTo("worker-a");
        assertThat(renewed.attempt()).isEqualTo(1);
        assertThat(renewed.finishedAt()).isNull();
        assertThat(renewed.error()).isNull();
        assertThat(original.leaseDeadline()).as("records are immutable").isEqualTo(DEADLINE);
    }

    @Test
    void renewedWithoutProgressKeepsThePreviousValue() {
        Delivery at40 = leased().renewed(DEADLINE.plusSeconds(1), T0.plusSeconds(1), 40);
        Delivery again = at40.renewed(DEADLINE.plusSeconds(2), T0.plusSeconds(2), null);
        assertThat(again.progress()).isEqualTo(40);
        assertThat(again.heartbeatAt()).isEqualTo(T0.plusSeconds(2));

        Delivery never = leased().renewed(DEADLINE.plusSeconds(1), T0.plusSeconds(1), null);
        assertThat(never.progress()).isNull();
    }

    @ParameterizedTest
    @EnumSource(value = AckState.class, names = "LEASED", mode = EnumSource.Mode.EXCLUDE)
    void renewingAClosedDeliveryThrows(AckState closedAs) {
        Delivery closed = leased().close(closedAs, null, T0.plusSeconds(1));
        assertThatThrownBy(() -> closed.renewed(DEADLINE.plusSeconds(5), T0.plusSeconds(2), 10))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(closedAs.name());
    }

    // ---------------------------------------------------------------------------------- close

    @Test
    void closeAsAckedSetsProgressTo100() {
        Delivery at40 = leased().renewed(DEADLINE, T0.plusSeconds(5), 40);
        Instant end = T0.plusSeconds(8);
        Delivery acked = at40.close(AckState.ACKED, null, end);

        assertThat(acked.ackState()).isEqualTo(AckState.ACKED);
        assertThat(acked.progress()).isEqualTo(100);
        assertThat(acked.finishedAt()).isEqualTo(end);
        assertThat(acked.error()).isNull();
        assertThat(acked.heartbeatAt()).as("closing is not a heartbeat").isEqualTo(T0.plusSeconds(5));
        assertThat(acked.leaseDeadline()).isEqualTo(DEADLINE);
        assertThat(acked.startedAt()).isEqualTo(T0);
        assertThat(acked.queuedAt()).isEqualTo(QUEUED_AT);
    }

    @Test
    void closeAsAckedWithoutAnyReportedProgressStillSetsProgressTo100() {
        assertThat(leased().close(AckState.ACKED, null, T0.plusSeconds(1)).progress()).isEqualTo(100);
    }

    @ParameterizedTest
    @EnumSource(value = AckState.class, names = {"NACKED", "EXPIRED", "CANCELLED"})
    void closeWithAnyOtherOutcomeKeepsProgressAndRecordsTheError(AckState outcome) {
        Instant end = T0.plusSeconds(9);
        Delivery closed = leased().renewed(DEADLINE, T0.plusSeconds(4), 60).close(outcome, "why", end);

        assertThat(closed.ackState()).isEqualTo(outcome);
        assertThat(closed.progress()).isEqualTo(60);
        assertThat(closed.error()).isEqualTo("why");
        assertThat(closed.finishedAt()).isEqualTo(end);
        assertThat(closed.heartbeatAt()).isEqualTo(T0.plusSeconds(4));

        Delivery noProgress = leased().close(outcome, null, end);
        assertThat(noProgress.progress()).isNull();
        assertThat(noProgress.error()).isNull();
    }

    static Stream<Arguments> closedThenClosedAgain() {
        List<Arguments> cases = new ArrayList<>();
        for (AckState first : AckState.values()) {
            if (first == AckState.LEASED) continue;
            for (AckState second : AckState.values()) cases.add(Arguments.of(first, second));
        }
        return cases.stream();
    }

    @ParameterizedTest(name = "{0} then {1}")
    @MethodSource("closedThenClosedAgain")
    void closingAClosedDeliveryThrows(AckState first, AckState second) {
        Delivery closed = leased().close(first, null, T0.plusSeconds(1));
        assertThatThrownBy(() -> closed.close(second, "again", T0.plusSeconds(2)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("delivery " + JOB + "#1 is " + first);
    }

    /**
     * {@code LEASED} is not an outcome: {@link AckState#canTransitionTo} forbids LEASED → LEASED, so {@code close}
     * must refuse it instead of producing an "open" delivery that carries a {@code finishedAt}.
     */
    @Test
    void closeRejectsLeasedAsAnOutcome() {
        Delivery open = leased();
        assertThat(AckState.LEASED.canTransitionTo(AckState.LEASED)).isFalse();
        assertThatThrownBy(() -> open.close(AckState.LEASED, null, T0.plusSeconds(1)))
                .isInstanceOfAny(IllegalStateException.class, IllegalArgumentException.class);
    }

    // ------------------------------------------------------------------------------ isExpired

    @Test
    void isExpiredBoundaryIsInclusiveAtTheDeadline() {
        Delivery d = leased();
        assertThat(d.isExpired(T0)).isFalse();
        assertThat(d.isExpired(DEADLINE.minusNanos(1))).isFalse();
        assertThat(d.isExpired(DEADLINE)).isTrue();
        assertThat(d.isExpired(DEADLINE.plusNanos(1))).isTrue();
        assertThat(d.isExpired(DEADLINE.plusSeconds(3600))).isTrue();
    }

    @Test
    void renewalPushesTheExpiryBoundary() {
        Delivery renewed = leased().renewed(DEADLINE.plusSeconds(30), T0.plusSeconds(20), null);
        assertThat(renewed.isExpired(DEADLINE)).isFalse();
        assertThat(renewed.isExpired(DEADLINE.plusSeconds(30))).isTrue();
    }

    @ParameterizedTest
    @EnumSource(value = AckState.class, names = "LEASED", mode = EnumSource.Mode.EXCLUDE)
    void aClosedDeliveryNeverExpires(AckState closedAs) {
        Delivery closed = leased().close(closedAs, null, T0.plusSeconds(1));
        assertThat(closed.isExpired(DEADLINE)).isFalse();
        assertThat(closed.isExpired(DEADLINE.plusSeconds(3600))).isFalse();
    }

    // ----------------------------------------------------------------------------- validation

    @Test
    void constructorRejectsMissingRequiredComponents() {
        assertThatThrownBy(() -> new Delivery(null, 1, "w", DEADLINE, AckState.LEASED, null, T0, T0, null, null, null))
                .isInstanceOf(NullPointerException.class).hasMessage("jobId");
        assertThatThrownBy(() -> new Delivery(JOB, 1, null, DEADLINE, AckState.LEASED, null, T0, T0, null, null, null))
                .isInstanceOf(NullPointerException.class).hasMessage("leaseOwner");
        assertThatThrownBy(() -> new Delivery(JOB, 1, "w", null, AckState.LEASED, null, T0, T0, null, null, null))
                .isInstanceOf(NullPointerException.class).hasMessage("leaseDeadline");
        assertThatThrownBy(() -> new Delivery(JOB, 1, "w", DEADLINE, null, null, T0, T0, null, null, null))
                .isInstanceOf(NullPointerException.class).hasMessage("ackState");
        assertThatThrownBy(() -> new Delivery(JOB, 1, "w", DEADLINE, AckState.LEASED, null, null, T0, null, null,
                null)).isInstanceOf(NullPointerException.class).hasMessage("startedAt");
    }

    @Test
    void constructorRejectsAttemptBelowOne() {
        assertThatThrownBy(() -> Delivery.lease(JOB, 0, "w", QUEUED_AT, T0, DEADLINE))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("attempt must be >= 1");
        assertThatThrownBy(() -> Delivery.lease(JOB, -5, "w", QUEUED_AT, T0, DEADLINE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(Delivery.lease(JOB, 1, "w", QUEUED_AT, T0, DEADLINE).attempt()).isEqualTo(1);
    }

    @Test
    void optionalComponentsMayBeNull() {
        Delivery d = new Delivery(JOB, 2, "w", DEADLINE, AckState.NACKED, null, T0, null, null, null, null);
        assertThat(d.queuedAt()).isNull();
        assertThat(d.heartbeatAt()).isNull();
        assertThat(d.finishedAt()).isNull();
        assertThat(d.progress()).isNull();
        assertThat(d.error()).isNull();
    }
}
