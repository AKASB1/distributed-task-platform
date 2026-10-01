package io.akasb.taskplatform.persistence;

import static io.akasb.taskplatform.domain.JobState.CANCELLED;
import static io.akasb.taskplatform.domain.JobState.DEAD_LETTER;
import static io.akasb.taskplatform.domain.JobState.FAILED;
import static io.akasb.taskplatform.domain.JobState.PENDING;
import static io.akasb.taskplatform.domain.JobState.QUEUED;
import static io.akasb.taskplatform.domain.JobState.RETRY_WAIT;
import static io.akasb.taskplatform.domain.JobState.RUNNING;
import static io.akasb.taskplatform.domain.JobState.SUCCEEDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.akasb.taskplatform.domain.AckState;
import io.akasb.taskplatform.domain.Delivery;
import io.akasb.taskplatform.domain.Job;
import io.akasb.taskplatform.domain.JobState;
import io.akasb.taskplatform.domain.Lease;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Behaviour every {@link JobRepository} must share: field round trips, idempotent inserts, the compare-and-set guards
 * of every mutating method (a rejected write leaves every row unchanged) and the filters, ordering and limits of the
 * reconciler scans. Not run by itself; each implementation extends it and supplies a fresh, empty repository.
 */
abstract class JobRepositoryContract {
    /** Base instant with a non-zero microsecond part; every timestamp here is microsecond-aligned. */
    protected static final Instant T0 = Instant.parse("2026-03-01T10:00:00.123456Z");
    protected static final String QUEUE = "default";
    protected static final String PAYLOAD = "{\"durationMs\":10}";
    private static final ObjectMapper JSON = new ObjectMapper();

    private final Random random = new Random(20260301L);
    protected JobRepository repository;

    /** A new, empty repository. Called before every test. */
    protected abstract JobRepository newRepository();

    @BeforeEach
    void createRepository() {
        repository = newRepository();
    }

    // ---------------------------------------------------------------- insert / find

    @Test
    void insertAndFindRoundTripEveryField() {
        Job job = new Job(newId(), "images", "resize",
                "{\"width\":640,\"tags\":[\"a\",\"b\"],\"nested\":{\"ratio\":1.5,\"ok\":true,\"none\":null},"
                        + "\"text\":\"ünï \\\"q\\\"\"}",
                RETRY_WAIT, 5_000_000_007L, 2, 5, Duration.ofMillis(90_250), "key-every-field",
                T0, T0.plusNanos(1_000), t(1).plusNanos(999_000), t(2).minusNanos(1_000), t(3), t(4),
                "boom: \"quoted\" ünïcode\nsecond line", "{\"out\":[1,2,3],\"empty\":{}}");

        InsertResult result = repository.insert(job);

        assertThat(result.created()).isTrue();
        assertSameJob(result.job(), job);
        assertSameJob(stored(job.id()), job);
        assertSameJob(repository.findByIdempotencyKey("key-every-field").orElseThrow(), job);
    }

    @Test
    void insertAndFindRoundTripWithEveryOptionalFieldNull() {
        Job job = Job.pending(newId(), QUEUE, "sleep", "{}", 1, Duration.ofMillis(1), null, T0);

        assertThat(repository.insert(job)).isEqualTo(new InsertResult(job, true));

        Job found = stored(job.id());
        assertSameJob(found, job);
        assertThat(found.idempotencyKey()).isNull();
        assertThat(found.readyAt()).isNull();
        assertThat(found.dispatchedAt()).isNull();
        assertThat(found.nextAttemptAt()).isNull();
        assertThat(found.finishedAt()).isNull();
        assertThat(found.lastError()).isNull();
        assertThat(found.result()).isNull();
    }

    @Test
    void findOfUnknownIdOrKeyIsEmpty() {
        insertNew(Job.pending(newId(), QUEUE, "sleep", PAYLOAD, 3, Duration.ofSeconds(30), "known", T0));

        assertThat(repository.find(newId())).isEmpty();
        assertThat(repository.findByIdempotencyKey("unknown")).isEmpty();
    }

    @Test
    void jobsWithoutIdempotencyKeyAreAllCreated() {
        Job a = insertPending(QUEUE, t(0));
        Job b = insertPending(QUEUE, t(1));

        assertSameJob(stored(a.id()), a);
        assertSameJob(stored(b.id()), b);
        assertThat(total(repository.countByState())).isEqualTo(2);
    }

    @Test
    void insertWithTakenIdempotencyKeyReturnsTheStoredJobWithoutCreatingOne() {
        Job first = insertNew(Job.pending(newId(), QUEUE, "sleep", "{\"n\":1}", 3, Duration.ofSeconds(30), "key-1",
                t(0)));
        Job queued = write(first, first.enqueue(t(1), t(1)));

        Job duplicate = Job.pending(newId(), "other", "flaky", "{\"n\":2}", 5, Duration.ofSeconds(9), "key-1", t(2));
        InsertResult result = repository.insert(duplicate);

        assertThat(result.created()).isFalse();
        assertSameJob(result.job(), queued); // the current stored snapshot, not the submitted one
        assertThat(repository.find(duplicate.id())).isEmpty();
        assertSameJob(stored(first.id()), queued);
        assertSameJob(repository.findByIdempotencyKey("key-1").orElseThrow(), queued);
        assertThat(total(repository.countByState())).isEqualTo(1);
    }

    @Test
    void insertWithAnExistingIdIsRejectedAndDoesNotOverwrite() {
        Job original = insertPending(QUEUE, t(0));
        Job clash = new Job(original.id(), "other", "flaky", "{\"x\":1}", PENDING, 0, 0, 1, Duration.ofSeconds(1),
                null, t(5), t(5), null, null, null, null, null, null);

        assertThatThrownBy(() -> repository.insert(clash)).isInstanceOf(RuntimeException.class);

        assertSameJob(stored(original.id()), original);
    }

    // ---------------------------------------------------------------- update

    @Test
    void updateWritesEveryMutableFieldThroughALifecycle() {
        Job pending = insertPending(QUEUE, t(0));
        UUID id = pending.id();

        Job queued = write(pending, pending.enqueue(t(1), t(1)));
        assertSameJob(stored(id), queued);
        Job running = write(queued, queued.start(t(2)));
        assertSameJob(stored(id), running);
        Job retrying = write(running, running.retryAt(t(10), "transient: timeout", t(3)));
        assertSameJob(stored(id), retrying);
        Job requeued = write(retrying, retrying.enqueue(t(10), t(11)));
        assertSameJob(stored(id), requeued);
        assertThat(stored(id).nextAttemptAt()).isNull();
        Job runningAgain = write(requeued, requeued.start(t(12)));
        Job succeeded = write(runningAgain, runningAgain.succeed("{\"rows\":3}", t(13)));

        Job found = stored(id);
        assertSameJob(found, succeeded);
        assertThat(found.version()).isEqualTo(6);
        assertThat(found.attempts()).isEqualTo(2);
        assertThat(found.lastError()).isNull();
        assertThat(found.finishedAt()).isEqualTo(t(13));
    }

    @Test
    void updateCanClearEveryNullableMutableColumn() {
        Job full = insertNew(new Job(newId(), QUEUE, "sleep", PAYLOAD, RETRY_WAIT, 7, 2, 3, Duration.ofSeconds(30),
                "key-clear", t(0), t(1), t(2), t(3), t(4), t(5), "err", "{\"r\":1}"));
        Job cleared = new Job(full.id(), full.queue(), full.type(), full.payload(), QUEUED, 8, 2, 3, full.timeout(),
                full.idempotencyKey(), full.createdAt(), t(6), null, null, null, null, null, null);

        assertThat(repository.update(cleared, 7)).isTrue();

        assertSameJob(stored(full.id()), cleared);
    }

    @Test
    void updateWithStaleOrFutureVersionReturnsFalseAndLeavesTheRowUnchanged() {
        Job pending = insertPending(QUEUE, t(0));
        Job queued = write(pending, pending.enqueue(t(1), t(1)));

        assertThat(repository.update(pending.cancel(t(2)), pending.version())).isFalse();
        assertThat(repository.update(queued.cancel(t(2)), queued.version() + 1)).isFalse();

        assertSameJob(stored(pending.id()), queued);
    }

    @Test
    void updateOfAnUnknownJobReturnsFalse() {
        Job never = newPending(QUEUE, t(0));

        assertThat(repository.update(never.enqueue(t(1), t(1)), never.version())).isFalse();

        assertThat(repository.find(never.id())).isEmpty();
    }

    // ---------------------------------------------------------------- claim

    @Test
    void claimMovesTheJobToRunningAndOpensTheDelivery() {
        Job queued = insertQueued(QUEUE, t(0));
        Job running = queued.start(t(2));
        Delivery delivery = Delivery.lease(queued.id(), 1, "worker-a", queued.readyAt(), t(2), t(32));

        assertThat(repository.claim(running, queued.version(), delivery)).isTrue();

        assertSameJob(stored(queued.id()), running);
        assertThat(repository.deliveries(queued.id())).containsExactly(delivery);
    }

    @Test
    void claimWithStaleVersionReturnsFalseAndOpensNoDelivery() {
        Job pending = insertPending(QUEUE, t(0));
        Job queued = write(pending, pending.enqueue(t(1), t(1)));
        Delivery delivery = Delivery.lease(queued.id(), 1, "worker-a", queued.readyAt(), t(2), t(32));

        assertThat(repository.claim(queued.start(t(2)), pending.version(), delivery)).isFalse();

        assertSameJob(stored(queued.id()), queued);
        assertThat(repository.deliveries(queued.id())).isEmpty();
    }

    @Test
    void secondClaimOfTheSameSnapshotLosesAndKeepsTheWinnersDelivery() {
        Job queued = insertQueued(QUEUE, t(0));
        Lease winner = claim(queued, "worker-a", t(2), t(32));
        Delivery loser = Delivery.lease(queued.id(), 1, "worker-b", queued.readyAt(), t(3), t(33));

        assertThat(repository.claim(queued.start(t(3)), queued.version(), loser)).isFalse();

        assertSameJob(stored(queued.id()), winner.job());
        assertThat(repository.deliveries(queued.id())).containsExactly(winner.delivery());
    }

    @Test
    void claimWithDuplicateAttemptReturnsFalseAndLeavesTheJobUnchanged() {
        Job queued = insertQueued(QUEUE, t(0));
        Lease first = claim(queued, "worker-a", t(1), t(31));
        Job retrying = first.job().retryAt(t(5), "transient", t(2));
        Delivery nacked = first.delivery().close(AckState.NACKED, "transient", t(2));
        assertThat(repository.finishAttempt(retrying, first.job().version(), nacked, null)).isTrue();
        Job requeued = write(retrying, retrying.enqueue(t(5), t(5)));

        // The job CAS alone would succeed; the delivery insert collides with attempt 1, so nothing may change.
        Delivery duplicate = Delivery.lease(queued.id(), 1, "worker-b", t(5), t(6), t(36));
        assertThat(repository.claim(requeued.start(t(6)), requeued.version(), duplicate)).isFalse();

        assertSameJob(stored(queued.id()), requeued);
        assertThat(repository.deliveries(queued.id())).containsExactly(nacked);
    }

    // ---------------------------------------------------------------- finishAttempt

    @Test
    void finishAttemptWritesTheJobAndClosesTheDelivery() {
        Lease lease = insertRunning("worker-a", t(31));
        Job succeeded = lease.job().succeed("{\"ok\":true}", t(5));
        Delivery acked = lease.delivery().close(AckState.ACKED, null, t(5));

        assertThat(repository.finishAttempt(succeeded, lease.job().version(), acked, null)).isTrue();

        assertSameJob(stored(lease.job().id()), succeeded);
        assertThat(repository.deliveries(lease.job().id())).containsExactly(acked);
    }

    @Test
    void finishAttemptWithStaleVersionLeavesTheDeliveryOpen() {
        Lease lease = insertRunning("worker-a", t(31));
        Job cancelled = write(lease.job(), lease.job().cancel(t(3)));

        boolean finished = repository.finishAttempt(lease.job().succeed("{}", t(5)), lease.job().version(),
                lease.delivery().close(AckState.ACKED, null, t(5)), null);

        assertThat(finished).isFalse();
        assertSameJob(stored(lease.job().id()), cancelled);
        assertThat(repository.deliveries(lease.job().id())).containsExactly(lease.delivery());
    }

    @Test
    void finishAttemptIsAtomicWhenTheDeliveryIsAlreadyClosed() {
        Lease lease = insertRunning("worker-a", t(31));
        Delivery cancelled = lease.delivery().close(AckState.CANCELLED, "job cancelled", t(3));
        assertThat(repository.closeDelivery(cancelled, null)).isTrue();

        // closeDelivery left the job alone, so the job CAS would succeed: the delivery guard must veto it.
        boolean finished = repository.finishAttempt(lease.job().succeed("{}", t(5)), lease.job().version(),
                lease.delivery().close(AckState.ACKED, null, t(5)), null);

        assertThat(finished).isFalse();
        assertSameJob(stored(lease.job().id()), lease.job());
        assertThat(repository.deliveries(lease.job().id())).containsExactly(cancelled);
    }

    @Test
    void finishAttemptIsAtomicWhenAnotherWorkerOwnsTheDelivery() {
        Lease lease = insertRunning("worker-a", t(31));
        Delivery impostor = Delivery.lease(lease.job().id(), 1, "worker-b", lease.delivery().queuedAt(),
                lease.delivery().startedAt(), lease.deadline()).close(AckState.ACKED, null, t(5));

        boolean finished = repository.finishAttempt(lease.job().succeed("{}", t(5)), lease.job().version(),
                impostor, null);

        assertThat(finished).isFalse();
        assertSameJob(stored(lease.job().id()), lease.job());
        assertThat(repository.deliveries(lease.job().id())).containsExactly(lease.delivery());
    }

    @Test
    void finishAttemptIsAtomicForAnAttemptWithoutDelivery() {
        Lease lease = insertRunning("worker-a", t(31));
        Delivery otherAttempt = Delivery.lease(lease.job().id(), 2, "worker-a", lease.delivery().queuedAt(),
                lease.delivery().startedAt(), lease.deadline()).close(AckState.ACKED, null, t(5));

        boolean finished = repository.finishAttempt(lease.job().succeed("{}", t(5)), lease.job().version(),
                otherAttempt, null);

        assertThat(finished).isFalse();
        assertSameJob(stored(lease.job().id()), lease.job());
        assertThat(repository.deliveries(lease.job().id())).containsExactly(lease.delivery());
    }

    @Test
    void finishAttemptWithExpiryGuardLosesToALeaseRenewedMeanwhile() {
        Lease lease = insertRunning("worker-a", t(30));
        UUID id = lease.job().id();
        assertThat(repository.renewLease(id, 1, "worker-a", lease.job().version(), t(60), t(29), 10)).isTrue();
        Delivery renewed = lease.delivery().renewed(t(60), t(29), 10);

        // The reconciler scanned before the renewal and still holds the old snapshot (deadline t+30s).
        Job retrying = lease.job().retryAt(t(50), "lease expired", t(45));
        Delivery expired = lease.delivery().close(AckState.EXPIRED, "lease expired", t(45));
        assertThat(repository.finishAttempt(retrying, lease.job().version(), expired, t(40))).isFalse();

        assertSameJob(stored(id), lease.job());
        assertThat(repository.deliveries(id)).containsExactly(renewed);
    }

    @Test
    void finishAttemptWithExpiryGuardNeedsTheDeadlineStrictlyBeforeTheCutoff() {
        Lease lease = insertRunning("worker-a", t(30));
        UUID id = lease.job().id();
        Job retrying = lease.job().retryAt(t(50), "lease expired", t(45));
        Delivery expired = lease.delivery().close(AckState.EXPIRED, "lease expired", t(45));

        assertThat(repository.finishAttempt(retrying, lease.job().version(), expired, t(30))).isFalse();
        assertSameJob(stored(id), lease.job());
        assertThat(repository.deliveries(id)).containsExactly(lease.delivery());

        assertThat(repository.finishAttempt(retrying, lease.job().version(), expired, t(30).plusNanos(1_000)))
                .isTrue();
        assertSameJob(stored(id), retrying);
        assertThat(repository.deliveries(id)).containsExactly(expired);
    }

    // ---------------------------------------------------------------- renewLease

    @Test
    void renewLeaseExtendsTheDeadlineAndKeepsProgressWhenNull() {
        Lease lease = insertRunning("worker-a", t(31));
        UUID id = lease.job().id();
        long version = lease.job().version();

        assertThat(repository.renewLease(id, 1, "worker-a", version, t(61), t(31), 40)).isTrue();
        Delivery first = lease.delivery().renewed(t(61), t(31), 40);
        assertThat(repository.deliveries(id)).containsExactly(first);

        assertThat(repository.renewLease(id, 1, "worker-a", version, t(91), t(61), null)).isTrue();
        Delivery second = first.renewed(t(91), t(61), null);
        assertThat(second.progress()).isEqualTo(40);
        assertThat(repository.deliveries(id)).containsExactly(second);

        // Renewals write the delivery only: the job version stays the fencing token for the attempt.
        assertSameJob(stored(id), lease.job());
    }

    @Test
    void renewLeaseRejectsAnotherOwner() {
        Lease lease = insertRunning("worker-a", t(31));

        assertThat(repository.renewLease(lease.job().id(), 1, "worker-b", lease.job().version(), t(61), t(31), 50))
                .isFalse();

        assertThat(repository.deliveries(lease.job().id())).containsExactly(lease.delivery());
    }

    @Test
    void renewLeaseRejectsAnotherAttempt() {
        Lease lease = insertRunning("worker-a", t(31));

        assertThat(repository.renewLease(lease.job().id(), 2, "worker-a", lease.job().version(), t(61), t(31), 50))
                .isFalse();

        assertThat(repository.deliveries(lease.job().id())).containsExactly(lease.delivery());
    }

    @Test
    void renewLeaseRejectsAChangedJobVersion() {
        Lease lease = insertRunning("worker-a", t(31));
        Job cancelled = write(lease.job(), lease.job().cancel(t(3)));

        assertThat(repository.renewLease(lease.job().id(), 1, "worker-a", lease.job().version(), t(61), t(31), 50))
                .isFalse();

        assertThat(repository.deliveries(lease.job().id())).containsExactly(lease.delivery());
        assertSameJob(stored(lease.job().id()), cancelled);
    }

    @Test
    void renewLeaseRejectsAJobThatIsNoLongerRunningEvenAtItsCurrentVersion() {
        Lease lease = insertRunning("worker-a", t(31));
        Job cancelled = write(lease.job(), lease.job().cancel(t(3)));

        assertThat(repository.renewLease(lease.job().id(), 1, "worker-a", cancelled.version(), t(61), t(31), 50))
                .isFalse();

        assertThat(repository.deliveries(lease.job().id())).containsExactly(lease.delivery());
        assertSameJob(stored(lease.job().id()), cancelled);
    }

    @Test
    void renewLeaseRejectsAClosedDelivery() {
        Lease lease = insertRunning("worker-a", t(31));
        Delivery closed = lease.delivery().close(AckState.CANCELLED, "job cancelled", t(3));
        assertThat(repository.closeDelivery(closed, null)).isTrue();

        // The job is still RUNNING at the lease's version; only the delivery guard can refuse.
        assertThat(repository.renewLease(lease.job().id(), 1, "worker-a", lease.job().version(), t(61), t(31), 50))
                .isFalse();

        assertThat(repository.deliveries(lease.job().id())).containsExactly(closed);
        assertSameJob(stored(lease.job().id()), lease.job());
    }

    @Test
    void renewLeaseOfAnUnknownJobReturnsFalse() {
        assertThat(repository.renewLease(newId(), 1, "worker-a", 2, t(61), t(31), null)).isFalse();
    }

    // ---------------------------------------------------------------- closeDelivery

    @Test
    void closeDeliveryClosesTheOpenDeliveryWithoutTouchingTheJob() {
        Lease lease = insertRunning("worker-a", t(31));
        Job cancelled = write(lease.job(), lease.job().cancel(t(3)));
        Delivery closed = lease.delivery().close(AckState.CANCELLED, "job cancelled", t(4));

        assertThat(repository.closeDelivery(closed, null)).isTrue();

        assertThat(repository.deliveries(lease.job().id())).containsExactly(closed);
        assertSameJob(stored(lease.job().id()), cancelled);
    }

    @Test
    void closeDeliveryRejectsAnotherOwner() {
        Lease lease = insertRunning("worker-a", t(31));
        Delivery impostor = Delivery.lease(lease.job().id(), 1, "worker-b", lease.delivery().queuedAt(),
                lease.delivery().startedAt(), lease.deadline()).close(AckState.CANCELLED, "job cancelled", t(4));

        assertThat(repository.closeDelivery(impostor, null)).isFalse();

        assertThat(repository.deliveries(lease.job().id())).containsExactly(lease.delivery());
        assertSameJob(stored(lease.job().id()), lease.job());
    }

    @Test
    void closeDeliveryRejectsAnAlreadyClosedDelivery() {
        Lease lease = insertRunning("worker-a", t(31));
        Delivery cancelled = lease.delivery().close(AckState.CANCELLED, "job cancelled", t(4));
        assertThat(repository.closeDelivery(cancelled, null)).isTrue();

        Delivery expired = lease.delivery().close(AckState.EXPIRED, "lease expired", t(40));
        assertThat(repository.closeDelivery(expired, null)).isFalse();

        assertThat(repository.deliveries(lease.job().id())).containsExactly(cancelled);
    }

    @Test
    void closeDeliveryWithExpiryGuardNeedsAnExpiredUnrenewedLease() {
        Lease lease = insertRunning("worker-a", t(30));
        UUID id = lease.job().id();
        Delivery staleExpired = lease.delivery().close(AckState.EXPIRED, "lease expired", t(45));

        assertThat(repository.closeDelivery(staleExpired, t(30))).isFalse(); // deadline equal to the cutoff
        assertThat(repository.deliveries(id)).containsExactly(lease.delivery());

        assertThat(repository.renewLease(id, 1, "worker-a", lease.job().version(), t(60), t(29), null)).isTrue();
        Delivery renewed = lease.delivery().renewed(t(60), t(29), null);
        assertThat(repository.closeDelivery(staleExpired, t(40))).isFalse(); // renewed past the cutoff
        assertThat(repository.deliveries(id)).containsExactly(renewed);

        Delivery expired = renewed.close(AckState.EXPIRED, "lease expired", t(70));
        assertThat(repository.closeDelivery(expired, t(61))).isTrue();
        assertThat(repository.deliveries(id)).containsExactly(expired);
        assertSameJob(stored(id), lease.job());
    }

    @Test
    void closeDeliveryOfAnUnknownDeliveryReturnsFalse() {
        Lease lease = insertRunning("worker-a", t(31));
        Delivery unknownJob = Delivery.lease(newId(), 1, "worker-a", null, t(1), t(31))
                .close(AckState.EXPIRED, "lease expired", t(40));
        Delivery unknownAttempt = Delivery.lease(lease.job().id(), 2, "worker-a", null, t(1), t(31))
                .close(AckState.EXPIRED, "lease expired", t(40));

        assertThat(repository.closeDelivery(unknownJob, null)).isFalse();
        assertThat(repository.closeDelivery(unknownAttempt, null)).isFalse();

        assertThat(repository.deliveries(unknownJob.jobId())).isEmpty();
        assertThat(repository.deliveries(lease.job().id())).containsExactly(lease.delivery());
    }

    // ---------------------------------------------------------------- markDispatched

    @Test
    void markDispatchedChangesOnlyTheDispatchTimeOfAQueuedJob() {
        Job queued = insertQueued(QUEUE, t(0));

        repository.markDispatched(queued.id(), t(9));

        Job found = stored(queued.id());
        assertSameJob(found, withDispatchedAt(queued, t(9)));
        assertThat(found.version()).isEqualTo(queued.version());
        assertThat(found.updatedAt()).isEqualTo(queued.updatedAt());
        // The version is untouched, so a CAS on it still succeeds.
        assertThat(repository.update(queued.cancel(t(10)), queued.version())).isTrue();
    }

    @Test
    void markDispatchedIgnoresJobsThatAreNotQueued() {
        Job pending = insertPending(QUEUE, t(0));
        Job running = insertRunning("worker-a", t(31)).job();
        Job retrying = insertNew(snapshot(RETRY_WAIT, QUEUE, t(0), t(0), t(1), t(20)));
        Job cancelled = insertNew(snapshot(CANCELLED, QUEUE, t(0), t(0), t(1), null));
        Job succeeded = insertNew(snapshot(SUCCEEDED, QUEUE, t(0), t(0), t(1), null));

        for (Job job : List.of(pending, running, retrying, cancelled, succeeded)) {
            repository.markDispatched(job.id(), t(50));
        }
        repository.markDispatched(newId(), t(50)); // unknown id: a no-op

        for (Job job : List.of(pending, running, retrying, cancelled, succeeded)) {
            assertSameJob(stored(job.id()), job);
        }
    }

    // ---------------------------------------------------------------- deliveries

    @Test
    void deliveriesAreReturnedInAttemptOrder() {
        Job current = insertQueued(QUEUE, t(0));
        UUID id = current.id();
        List<Delivery> closed = new ArrayList<>();
        int second = 1;
        // Attempt numbers are written out of order so that storage order differs from attempt order.
        for (int attempt : new int[] {3, 1, 2}) {
            Job running = current.start(t(second));
            Delivery open = Delivery.lease(id, attempt, "worker-" + attempt, current.readyAt(), t(second),
                    t(second + 30));
            assertThat(repository.claim(running, current.version(), open)).isTrue();
            Job retrying = running.retryAt(t(second + 2), "transient", t(second + 1));
            Delivery nacked = open.close(AckState.NACKED, "transient", t(second + 1));
            assertThat(repository.finishAttempt(retrying, running.version(), nacked, null)).isTrue();
            current = write(retrying, retrying.enqueue(t(second + 2), t(second + 2)));
            closed.add(nacked);
            second += 10;
        }

        List<Delivery> deliveries = repository.deliveries(id);

        assertThat(deliveries).extracting(Delivery::attempt).containsExactly(1, 2, 3);
        assertThat(deliveries).containsExactly(closed.get(1), closed.get(2), closed.get(0));
        assertThat(repository.deliveries(newId())).isEmpty();
    }

    // ---------------------------------------------------------------- reconciler scans

    @Test
    void findExpiredLeasesReturnsOpenLeasesBeforeTheCutoffOldestFirst() {
        Delivery b = insertRunning("worker-b", t(20)).delivery();
        Delivery a = insertRunning("worker-a", t(10)).delivery();
        Lease closedLease = insertRunning("worker-c", t(15));
        assertThat(repository.closeDelivery(closedLease.delivery().close(AckState.ACKED, null, t(2)), null)).isTrue();
        Delivery atCutoff = insertRunning("worker-d", t(40)).delivery();
        Delivery e = insertRunning("worker-e", t(50)).delivery();
        Delivery c = insertRunning("worker-f", t(30)).delivery();

        assertThat(repository.findExpiredLeases(t(40), 10)).containsExactly(a, b, c);
        assertThat(repository.findExpiredLeases(t(40), 2)).containsExactly(a, b);
        assertThat(repository.findExpiredLeases(t(10), 10)).isEmpty();
        assertThat(repository.findExpiredLeases(t(100), 10)).containsExactly(a, b, c, atCutoff, e);
    }

    @Test
    void findDueRetriesReturnsRetryWaitJobsDueAtOrBeforeNowSoonestFirst() {
        Job due20 = insertNew(snapshot(RETRY_WAIT, QUEUE, t(0), t(0), t(0), t(20)));
        Job due10 = insertNew(snapshot(RETRY_WAIT, "other", t(0), t(0), t(0), t(10)));
        Job dueNow = insertNew(snapshot(RETRY_WAIT, QUEUE, t(0), t(0), t(0), t(30)));
        insertNew(snapshot(RETRY_WAIT, QUEUE, t(0), t(0), t(0), t(30).plusNanos(1_000)));
        insertNew(snapshot(QUEUED, QUEUE, t(0), t(0), t(0), null));
        insertNew(snapshot(RUNNING, QUEUE, t(0), t(0), t(0), t(5)));
        insertNew(snapshot(CANCELLED, QUEUE, t(0), t(0), t(0), t(5)));

        assertThat(ids(repository.findDueRetries(t(30), 10))).containsExactly(due10.id(), due20.id(), dueNow.id());
        assertThat(ids(repository.findDueRetries(t(30), 2))).containsExactly(due10.id(), due20.id());
        assertThat(repository.findDueRetries(t(10).minusNanos(1_000), 10)).isEmpty();
        assertSameJob(repository.findDueRetries(t(10), 10).get(0), due10);
    }

    @Test
    void findStalePendingReturnsPendingJobsCreatedBeforeTheCutoffOldestFirst() {
        Job created5 = insertPending(QUEUE, t(5));
        Job created1 = insertPending("other", t(1));
        Job created3 = insertPending(QUEUE, t(3));
        insertPending(QUEUE, t(10));
        insertNew(snapshot(QUEUED, QUEUE, t(0), t(0), t(0), null));
        insertNew(snapshot(CANCELLED, QUEUE, t(0), null, null, null));

        assertThat(ids(repository.findStalePending(t(10), 10)))
                .containsExactly(created1.id(), created3.id(), created5.id());
        assertThat(ids(repository.findStalePending(t(10), 2))).containsExactly(created1.id(), created3.id());
        assertThat(repository.findStalePending(t(1), 10)).isEmpty();
        assertSameJob(repository.findStalePending(t(2), 10).get(0), created1);
    }

    @Test
    void findQueuedDispatchedBeforeReturnsQueuedJobsNeverOrLongAgoDispatchedOldestFirst() {
        Job dispatched5 = insertNew(snapshot(QUEUED, QUEUE, t(0), t(0), t(5), null));
        Job neverDispatched = insertNew(snapshot(QUEUED, QUEUE, t(0), t(0), null, null));
        Job dispatched1 = insertNew(snapshot(QUEUED, "other", t(0), t(0), t(1), null));
        Job atCutoff = insertNew(snapshot(QUEUED, QUEUE, t(0), t(0), t(10), null));
        Job dispatched20 = insertNew(snapshot(QUEUED, QUEUE, t(0), t(0), t(20), null));
        insertNew(snapshot(RUNNING, QUEUE, t(0), t(0), t(0), null));
        insertNew(snapshot(PENDING, QUEUE, t(0), null, null, null));
        insertNew(snapshot(RETRY_WAIT, QUEUE, t(0), t(0), t(0), t(1)));

        assertThat(ids(repository.findQueuedDispatchedBefore(t(10), 10)))
                .containsExactly(neverDispatched.id(), dispatched1.id(), dispatched5.id());
        assertThat(ids(repository.findQueuedDispatchedBefore(t(10), 2)))
                .containsExactly(neverDispatched.id(), dispatched1.id());
        // Startup recovery of a non-durable queue uses a cutoff just after "now": everything QUEUED qualifies.
        assertThat(ids(repository.findQueuedDispatchedBefore(t(20).plusNanos(1_000), 10)))
                .containsExactly(neverDispatched.id(), dispatched1.id(), dispatched5.id(), atCutoff.id(),
                        dispatched20.id());
        assertSameJob(repository.findQueuedDispatchedBefore(t(10), 1).get(0), neverDispatched);
    }

    @Test
    void markDispatchedMovesAJobOutOfTheRedispatchScan() {
        Job queued = insertQueued(QUEUE, t(0));
        assertThat(ids(repository.findQueuedDispatchedBefore(t(5), 10))).containsExactly(queued.id());

        repository.markDispatched(queued.id(), t(5));

        assertThat(repository.findQueuedDispatchedBefore(t(5), 10)).isEmpty();
    }

    // ---------------------------------------------------------------- stats

    @Test
    void queueStatsCountsTheQueueByStateAndFindsItsOldestQueuedJob() {
        insertNew(snapshot(PENDING, "alpha", t(0), null, null, null));
        insertNew(snapshot(PENDING, "alpha", t(1), null, null, null));
        insertNew(snapshot(QUEUED, "alpha", t(0), t(5), t(5), null));
        insertNew(snapshot(QUEUED, "alpha", t(0), t(2), t(2), null));
        insertNew(snapshot(QUEUED, "alpha", t(0), t(8), t(8), null));
        insertNew(snapshot(RUNNING, "alpha", t(0), t(0), t(0), null)); // older readyAt, but not QUEUED
        insertNew(snapshot(SUCCEEDED, "alpha", t(0), t(0), t(0), null));
        insertNew(snapshot(QUEUED, "beta", t(0), t(1), t(1), null)); // older, but another queue
        insertNew(snapshot(FAILED, "beta", t(0), t(0), t(0), null));

        QueueStats alpha = repository.queueStats("alpha", t(20));

        assertThat(alpha.queue()).isEqualTo("alpha");
        assertThat(alpha.now()).isEqualTo(t(20));
        assertThat(alpha.counts()).isEqualTo(allStates(Map.of(PENDING, 2L, QUEUED, 3L, RUNNING, 1L, SUCCEEDED, 1L)));
        assertThat(alpha.total()).isEqualTo(7);
        assertThat(alpha.oldestQueuedReadyAt()).isEqualTo(t(2));
        assertThat(alpha.oldestQueuedAge()).isEqualTo(Duration.ofSeconds(18));

        QueueStats beta = repository.queueStats("beta", t(20));

        assertThat(beta.counts()).isEqualTo(allStates(Map.of(QUEUED, 1L, FAILED, 1L)));
        assertThat(beta.oldestQueuedReadyAt()).isEqualTo(t(1));
    }

    @Test
    void queueStatsWithoutQueuedJobsHasNoOldestQueuedJob() {
        insertNew(snapshot(RUNNING, "busy", t(0), t(0), t(0), null));

        QueueStats unknown = repository.queueStats("nothing", t(1));
        QueueStats busy = repository.queueStats("busy", t(1));

        assertThat(unknown.counts()).isEqualTo(allStates(Map.of()));
        assertThat(unknown.total()).isZero();
        assertThat(unknown.oldestQueuedReadyAt()).isNull();
        assertThat(unknown.oldestQueuedAge()).isEqualTo(Duration.ZERO);
        assertThat(busy.counts()).isEqualTo(allStates(Map.of(RUNNING, 1L)));
        assertThat(busy.oldestQueuedReadyAt()).isNull();
    }

    @Test
    void countByStateCountsJobsOfEveryQueue() {
        assertThat(allStates(repository.countByState())).isEqualTo(allStates(Map.of()));

        insertNew(snapshot(PENDING, "alpha", t(0), null, null, null));
        insertNew(snapshot(PENDING, "alpha", t(1), null, null, null));
        insertNew(snapshot(PENDING, "beta", t(2), null, null, null));
        insertNew(snapshot(QUEUED, "alpha", t(0), t(0), t(0), null));
        insertNew(snapshot(RUNNING, "beta", t(0), t(0), t(0), null));
        insertNew(snapshot(DEAD_LETTER, "gamma", t(0), t(0), t(0), null));
        insertNew(snapshot(CANCELLED, "alpha", t(0), null, null, null));
        insertNew(snapshot(CANCELLED, "gamma", t(0), null, null, null));

        assertThat(allStates(repository.countByState())).isEqualTo(allStates(Map.of(
                PENDING, 3L, QUEUED, 1L, RUNNING, 1L, DEAD_LETTER, 1L, CANCELLED, 2L)));
    }

    // ---------------------------------------------------------------- helpers

    protected static Instant t(long seconds) {
        return T0.plusSeconds(seconds);
    }

    /** Ids come from a seeded generator so runs are reproducible. */
    protected UUID newId() {
        return new UUID(random.nextLong(), random.nextLong());
    }

    protected Job newPending(String queue, Instant createdAt) {
        return Job.pending(newId(), queue, "sleep", PAYLOAD, 3, Duration.ofSeconds(30), null, createdAt);
    }

    protected Job insertNew(Job job) {
        InsertResult result = repository.insert(job);
        assertThat(result.created()).as("insert of %s created", job.id()).isTrue();
        return result.job();
    }

    protected Job insertPending(String queue, Instant createdAt) {
        return insertNew(newPending(queue, createdAt));
    }

    /** A PENDING job moved to QUEUED (version 1) with readyAt and dispatchedAt at {@code at}. */
    protected Job insertQueued(String queue, Instant at) {
        Job pending = insertPending(queue, at);
        return write(pending, pending.enqueue(at, at));
    }

    /** A job claimed (version 2, attempt 1) by {@code owner} at t+1s with the given lease deadline. */
    protected Lease insertRunning(String owner, Instant deadline) {
        return claim(insertQueued(QUEUE, t(0)), owner, t(1), deadline);
    }

    protected Lease claim(Job queued, String owner, Instant at, Instant deadline) {
        Job running = queued.start(at);
        Delivery delivery = Delivery.lease(queued.id(), running.attempts(), owner, queued.readyAt(), at, deadline);
        assertThat(repository.claim(running, queued.version(), delivery)).as("claim").isTrue();
        return new Lease(running, delivery);
    }

    protected Job write(Job current, Job next) {
        assertThat(repository.update(next, current.version()))
                .as("update %s -> %s", current.state(), next.state()).isTrue();
        return next;
    }

    /** A snapshot built directly, for scans and stats that only care about a few columns. */
    protected Job snapshot(JobState state, String queue, Instant createdAt, Instant readyAt, Instant dispatchedAt,
                           Instant nextAttemptAt) {
        return new Job(newId(), queue, "sleep", PAYLOAD, state, 3, 1, 3, Duration.ofSeconds(30), null, createdAt,
                createdAt, readyAt, dispatchedAt, nextAttemptAt, null, null, null);
    }

    protected Job stored(UUID id) {
        return repository.find(id).orElseThrow(() -> new AssertionError("no job " + id));
    }

    /** Equal in every field; the JSON columns compare as JSON values, since a JSONB store normalises the text. */
    protected static void assertSameJob(Job actual, Job expected) {
        assertThat(actual).as("job").isNotNull();
        assertSameJson(actual.payload(), expected.payload());
        assertSameJson(actual.result(), expected.result());
        assertThat(withJson(actual, expected.payload(), expected.result())).isEqualTo(expected);
    }

    protected static void assertSameJson(String actual, String expected) {
        if (expected == null) {
            assertThat(actual).as("JSON").isNull();
            return;
        }
        assertThat(actual).as("JSON").isNotNull();
        assertThat(readJson(actual)).as("JSON %s vs %s", actual, expected).isEqualTo(readJson(expected));
    }

    protected static List<UUID> ids(List<Job> jobs) {
        return jobs.stream().map(Job::id).toList();
    }

    protected static long total(Map<JobState, Long> counts) {
        return counts.values().stream().mapToLong(Long::longValue).sum();
    }

    /** The counts with an explicit zero for every absent state. */
    protected static Map<JobState, Long> allStates(Map<JobState, Long> counts) {
        Map<JobState, Long> all = new EnumMap<>(JobState.class);
        for (JobState s : JobState.values()) all.put(s, counts.getOrDefault(s, 0L));
        return all;
    }

    private static Job withDispatchedAt(Job j, Instant dispatchedAt) {
        return new Job(j.id(), j.queue(), j.type(), j.payload(), j.state(), j.version(), j.attempts(),
                j.maxAttempts(), j.timeout(), j.idempotencyKey(), j.createdAt(), j.updatedAt(), j.readyAt(),
                dispatchedAt, j.nextAttemptAt(), j.finishedAt(), j.lastError(), j.result());
    }

    private static Job withJson(Job j, String payload, String result) {
        return new Job(j.id(), j.queue(), j.type(), payload, j.state(), j.version(), j.attempts(), j.maxAttempts(),
                j.timeout(), j.idempotencyKey(), j.createdAt(), j.updatedAt(), j.readyAt(), j.dispatchedAt(),
                j.nextAttemptAt(), j.finishedAt(), j.lastError(), result);
    }

    private static JsonNode readJson(String json) {
        try {
            return JSON.readTree(json);
        } catch (JsonProcessingException e) {
            throw new AssertionError("not JSON: " + json, e);
        }
    }
}
