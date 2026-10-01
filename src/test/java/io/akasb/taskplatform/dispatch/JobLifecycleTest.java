package io.akasb.taskplatform.dispatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.tuple;

import io.akasb.taskplatform.dispatch.Checkpoints.Point;
import io.akasb.taskplatform.domain.AckState;
import io.akasb.taskplatform.domain.Delivery;
import io.akasb.taskplatform.domain.FailureKind;
import io.akasb.taskplatform.domain.Job;
import io.akasb.taskplatform.domain.JobState;
import io.akasb.taskplatform.domain.Lease;
import io.akasb.taskplatform.domain.RetryPolicy;
import io.akasb.taskplatform.domain.TaskFailure;
import io.akasb.taskplatform.observability.LifecycleListener;
import io.akasb.taskplatform.persistence.InMemoryJobRepository;
import io.akasb.taskplatform.support.MutableClock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * {@link JobLifecycle} on the in-memory repository and dispatcher: submission, claim, heartbeat, completion, failure
 * and retry, cancellation, stale ("zombie") workers and the listener events. Time only moves through
 * {@link MutableClock}; the retry policy draws from a seeded random source.
 */
class JobLifecycleTest {
    static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
    static final Duration LEASE = Duration.ofSeconds(30);
    static final String QUEUE = "default";
    static final long SEED = 20260101L;
    private static final Reconciler.Settings RECONCILER = new Reconciler.Settings(Duration.ofSeconds(5),
            Duration.ofSeconds(10), Duration.ofSeconds(20), 100);
    private static final TaskFailure BOOM = new TaskFailure(FailureKind.RETRYABLE, "boom");

    private MutableClock clock;
    private InMemoryJobRepository repository;
    private InMemoryDispatcher dispatcher;
    private RecordingListener listener;
    private ScriptedCheckpoints checkpoints;
    private JobLifecycle lifecycle;
    private Reconciler reconciler;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(START);
        repository = new InMemoryJobRepository();
        dispatcher = new InMemoryDispatcher();
        listener = new RecordingListener();
        checkpoints = new ScriptedCheckpoints();
        lifecycle = new JobLifecycle(repository, dispatcher, retryPolicy(), clock, LEASE, listener, checkpoints);
        reconciler = new Reconciler(repository, lifecycle, dispatcher, RECONCILER);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (executor != null) {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    // ---------------------------------------------------------------- submission

    @Test
    void submitQueuesJobWithVersionOneAndDispatchesItOnce() {
        SubmitResult result = lifecycle.submit(command(3, null));
        Job job = result.job();

        assertThat(result.created()).isTrue();
        assertThat(job.state()).isEqualTo(JobState.QUEUED);
        assertThat(job.version()).isEqualTo(1);
        assertThat(job.attempts()).isZero();
        assertThat(job.maxAttempts()).isEqualTo(3);
        assertThat(job.createdAt()).isEqualTo(START);
        assertThat(job.updatedAt()).isEqualTo(START);
        assertThat(job.readyAt()).isEqualTo(START);
        assertThat(job.dispatchedAt()).isEqualTo(START);
        assertThat(job.nextAttemptAt()).isNull();
        assertThat(job.finishedAt()).isNull();
        assertThat(stored(job.id())).isEqualTo(job);
        assertThat(repository.deliveries(job.id())).isEmpty();
        assertThat(drain(dispatcher)).containsExactly(job.id());

        assertThat(listener.labels()).containsExactly("submitted");
        Job announced = listener.last("submitted").job();
        assertThat(announced.state()).isEqualTo(JobState.PENDING);
        assertThat(announced.version()).isZero();
    }

    @Test
    void timestampsAreTruncatedToMicroseconds() {
        clock.set(START.plusNanos(123_456_789));
        Instant micros = START.plusNanos(123_456_000);

        Job job = submit(3);
        Lease lease = claim(job.id(), "w1");

        assertThat(lifecycle.now()).isEqualTo(micros);
        assertThat(job.createdAt()).isEqualTo(micros);
        assertThat(job.readyAt()).isEqualTo(micros);
        assertThat(job.dispatchedAt()).isEqualTo(micros);
        assertThat(lease.delivery().startedAt()).isEqualTo(micros);
        assertThat(lease.deadline()).isEqualTo(micros.plus(LEASE));
    }

    @Test
    void idempotentSubmitReturnsExistingJobWithoutDispatching() {
        Job first = lifecycle.submit(command(3, "key-1")).job();
        assertThat(drain(dispatcher)).containsExactly(first.id());
        clock.advance(Duration.ofSeconds(1));

        SubmitResult again = lifecycle.submit(command(3, "key-1"));
        assertThat(again.created()).isFalse();
        assertThat(again.job()).isEqualTo(first);
        assertThat(dispatcher.depth(QUEUE)).isZero();

        // Once the job moved on, the stored snapshot is returned, still without a new dispatch.
        Lease lease = claim(first.id(), "w1");
        SubmitResult later = lifecycle.submit(command(3, "key-1"));
        assertThat(later.created()).isFalse();
        assertThat(later.job()).isEqualTo(lease.job());
        assertThat(dispatcher.depth(QUEUE)).isZero();
        assertThat(repository.countByState()).containsExactly(entry(JobState.RUNNING, 1L));
        assertThat(listener.count("submitted")).isEqualTo(1);
    }

    @Test
    void cancelThatWinsTheRaceWithSubmitIsReported() {
        checkpoints.on(Point.AFTER_PERSIST, lifecycle::cancel);

        SubmitResult result = lifecycle.submit(command(3, null));

        assertThat(result.created()).isTrue();
        assertThat(result.job().state()).isEqualTo(JobState.CANCELLED);
        assertThat(result.job()).isEqualTo(stored(result.job().id()));
        assertThat(dispatcher.depth(QUEUE)).isZero();
        assertThat(listener.labels()).containsExactly("submitted", "cancelled:PENDING");
    }

    // ---------------------------------------------------------------- claim

    @Test
    void claimLeasesQueuedJobExactlyOnce() {
        Job job = submit(3);
        clock.advance(Duration.ofSeconds(2));
        Instant claimedAt = START.plusSeconds(2);

        Lease lease = claim(job.id(), "worker-1");
        Job running = lease.job();
        assertThat(running.state()).isEqualTo(JobState.RUNNING);
        assertThat(running.version()).isEqualTo(2);
        assertThat(running.attempts()).isEqualTo(1);
        assertThat(running.readyAt()).isEqualTo(START);
        assertThat(running.updatedAt()).isEqualTo(claimedAt);

        Delivery delivery = lease.delivery();
        assertThat(delivery.jobId()).isEqualTo(job.id());
        assertThat(delivery.attempt()).isEqualTo(1);
        assertThat(delivery.leaseOwner()).isEqualTo("worker-1");
        assertThat(delivery.ackState()).isEqualTo(AckState.LEASED);
        assertThat(delivery.queuedAt()).isEqualTo(job.readyAt());
        assertThat(delivery.startedAt()).isEqualTo(claimedAt);
        assertThat(delivery.heartbeatAt()).isEqualTo(claimedAt);
        assertThat(delivery.leaseDeadline()).isEqualTo(claimedAt.plus(LEASE));
        assertThat(delivery.finishedAt()).isNull();
        assertThat(delivery.progress()).isNull();
        assertThat(delivery.error()).isNull();
        assertThat(stored(job.id())).isEqualTo(running);
        assertThat(repository.deliveries(job.id())).containsExactly(delivery);

        assertThat(lifecycle.claim(job.id(), "worker-2")).isEmpty();
        assertThat(lifecycle.claim(job.id(), "worker-1")).isEmpty();
        assertThat(stored(job.id())).isEqualTo(running);
        assertThat(repository.deliveries(job.id())).containsExactly(delivery);
        assertThat(listener.count("claimed")).isEqualTo(1);
    }

    @ParameterizedTest
    @EnumSource(value = JobState.class, names = {"PENDING", "RUNNING", "RETRY_WAIT", "SUCCEEDED", "FAILED",
            "DEAD_LETTER", "CANCELLED"})
    void claimIsEmptyUnlessJobIsQueued(JobState state) {
        Job job = jobIn(state);
        int deliveries = repository.deliveries(job.id()).size();
        long claims = listener.count("claimed");

        assertThat(lifecycle.claim(job.id(), "late-worker")).isEmpty();

        assertThat(stored(job.id())).isEqualTo(job);
        assertThat(repository.deliveries(job.id())).hasSize(deliveries);
        assertThat(listener.count("claimed")).isEqualTo(claims);
    }

    @Test
    void claimOfCancelledQueuedJobDropsTheStaleMessage() {
        Job job = submit(3);
        lifecycle.cancel(job.id());

        assertThat(drain(dispatcher)).containsExactly(job.id());
        assertThat(lifecycle.claim(job.id(), "w1")).isEmpty();
        assertThat(repository.deliveries(job.id())).isEmpty();
    }

    @Test
    void claimOfUnknownJobIsEmpty() {
        assertThat(lifecycle.claim(UUID.randomUUID(), "w1")).isEmpty();
    }

    // ---------------------------------------------------------------- heartbeat

    @Test
    void heartbeatRenewsLeaseAndStoresClampedProgress() {
        Lease lease = claim(submit(3).id(), "w1");
        UUID id = lease.job().id();
        clock.advance(Duration.ofSeconds(10));
        Instant beatAt = START.plusSeconds(10);

        HeartbeatResult first = lifecycle.heartbeat(lease, 40);
        assertThat(first.status()).isEqualTo(HeartbeatResult.Status.RENEWED);
        assertThat(first.lease().deadline()).isEqualTo(beatAt.plus(LEASE));
        assertThat(first.lease().delivery().heartbeatAt()).isEqualTo(beatAt);
        assertThat(first.lease().delivery().startedAt()).isEqualTo(START);
        assertThat(first.lease().delivery().progress()).isEqualTo(40);
        assertThat(first.lease().job()).isEqualTo(lease.job());
        assertThat(delivery(id, 1)).isEqualTo(first.lease().delivery());
        // A renewal writes the delivery row only: the job version (the attempt's fencing token) is unchanged.
        assertThat(stored(id)).isEqualTo(lease.job());

        clock.advance(Duration.ofSeconds(1));
        Lease high = renewed(first.lease(), 150);
        assertThat(high.delivery().progress()).isEqualTo(100);
        Lease unchanged = renewed(high, null);
        assertThat(unchanged.delivery().progress()).isEqualTo(100);
        Lease low = renewed(unchanged, -5);
        assertThat(low.delivery().progress()).isZero();
        assertThat(low.deadline()).isEqualTo(START.plusSeconds(11).plus(LEASE));
        assertThat(delivery(id, 1)).isEqualTo(low.delivery());
        assertThat(stored(id)).isEqualTo(lease.job());
        assertThat(listener.labels()).containsExactly("submitted", "claimed:1");
    }

    @Test
    void heartbeatAfterCancelTellsWorkerToStop() {
        Lease lease = claim(submit(3).id(), "w1");
        UUID id = lease.job().id();
        clock.advance(Duration.ofSeconds(3));
        Job cancelled = lifecycle.cancel(id);
        clock.advance(Duration.ofSeconds(3));

        HeartbeatResult result = lifecycle.heartbeat(lease, 50);

        assertThat(result.status()).isEqualTo(HeartbeatResult.Status.CANCELLED);
        assertThat(result.lease()).isNull();
        assertThat(delivery(id, 1)).isEqualTo(lease.delivery());
        assertThat(stored(id)).isEqualTo(cancelled);

        lifecycle.acknowledgeCancel(lease);
        Delivery closed = delivery(id, 1);
        assertThat(closed.ackState()).isEqualTo(AckState.CANCELLED);
        assertThat(closed.error()).isEqualTo("job cancelled");
        assertThat(closed.finishedAt()).isEqualTo(START.plusSeconds(6));
        assertThat(stored(id)).isEqualTo(cancelled);
    }

    @Test
    void heartbeatAfterLeaseWasReclaimedReportsLost() {
        Lease lease = claim(submit(3).id(), "w1");
        UUID id = lease.job().id();
        clock.advance(LEASE.plusSeconds(1));
        assertThat(lifecycle.expireLease(delivery(id, 1), lifecycle.now())).isTrue();
        assertThat(stored(id).state()).isEqualTo(JobState.RETRY_WAIT);
        Job afterReclaim = stored(id);

        HeartbeatResult result = lifecycle.heartbeat(lease, 90);

        assertThat(result.status()).isEqualTo(HeartbeatResult.Status.LOST);
        assertThat(result.lease()).isNull();
        assertThat(delivery(id, 1).ackState()).isEqualTo(AckState.EXPIRED);
        assertThat(delivery(id, 1).progress()).isNull();
        assertThat(stored(id)).isEqualTo(afterReclaim);
    }

    // ---------------------------------------------------------------- completion and failure

    @Test
    void completeSucceedsJobAndAcknowledgesDelivery() {
        Lease lease = claim(submit(3).id(), "w1");
        UUID id = lease.job().id();
        clock.advance(Duration.ofSeconds(5));
        Instant doneAt = START.plusSeconds(5);

        assertThat(lifecycle.complete(lease, "{\"ok\":true}")).isEqualTo(Outcome.ACCEPTED);

        Job done = stored(id);
        assertThat(done.state()).isEqualTo(JobState.SUCCEEDED);
        assertThat(done.version()).isEqualTo(3);
        assertThat(done.attempts()).isEqualTo(1);
        assertThat(done.result()).isEqualTo("{\"ok\":true}");
        assertThat(done.finishedAt()).isEqualTo(doneAt);
        assertThat(done.updatedAt()).isEqualTo(doneAt);
        assertThat(done.lastError()).isNull();
        Delivery acked = delivery(id, 1);
        assertThat(acked.ackState()).isEqualTo(AckState.ACKED);
        assertThat(acked.finishedAt()).isEqualTo(doneAt);
        assertThat(acked.progress()).isEqualTo(100);
        assertThat(acked.error()).isNull();
        assertThat(listener.labels()).containsExactly("submitted", "claimed:1", "attemptFinished:ACKED");
        assertThat(listener.last("attemptFinished").job()).isEqualTo(done);
        assertThat(listener.last("attemptFinished").delivery()).isEqualTo(acked);

        // A duplicate report from the same attempt is discarded.
        assertThat(lifecycle.complete(lease, "{\"again\":true}")).isEqualTo(Outcome.LEASE_LOST);
        assertThat(stored(id)).isEqualTo(done);
        assertThat(repository.deliveries(id)).containsExactly(acked);
    }

    @Test
    void reportsAfterCancelAreDiscardedAndJobStaysCancelled() {
        Lease lease = claim(submit(3).id(), "w1");
        UUID id = lease.job().id();
        clock.advance(Duration.ofSeconds(1));
        Job cancelled = lifecycle.cancel(id);
        clock.advance(Duration.ofSeconds(1));

        assertThat(lifecycle.complete(lease, "{}")).isEqualTo(Outcome.CANCELLED);
        assertThat(stored(id)).isEqualTo(cancelled);
        assertThat(stored(id).state()).isEqualTo(JobState.CANCELLED);
        assertThat(stored(id).result()).isNull();

        assertThat(lifecycle.fail(lease, BOOM)).isEqualTo(Outcome.CANCELLED);
        assertThat(stored(id)).isEqualTo(cancelled);
        assertThat(delivery(id, 1).ackState()).isNotIn(AckState.ACKED, AckState.NACKED);
        assertThat(listener.count("attemptFinished")).isZero();
        assertThat(listener.count("retryScheduled")).isZero();

        lifecycle.acknowledgeCancel(lease);
        assertThat(delivery(id, 1).ackState()).isEqualTo(AckState.CANCELLED);
        assertThat(stored(id)).isEqualTo(cancelled);
    }

    @Test
    void retryableFailureSchedulesRetryWithJitteredBackoff() {
        Duration expectedDelay = retryPolicy().backoff(1); // same seed, so the same first draw
        Lease lease = claim(submit(3).id(), "w1");
        UUID id = lease.job().id();
        drain(dispatcher);
        clock.advance(Duration.ofMillis(1500));
        Instant failedAt = lifecycle.now();

        assertThat(lifecycle.fail(lease, BOOM)).isEqualTo(Outcome.ACCEPTED);

        Job waiting = stored(id);
        assertThat(waiting.state()).isEqualTo(JobState.RETRY_WAIT);
        assertThat(waiting.version()).isEqualTo(3);
        assertThat(waiting.attempts()).isEqualTo(1);
        assertThat(waiting.lastError()).isEqualTo("RETRYABLE: boom");
        assertThat(waiting.finishedAt()).isNull();
        assertThat(waiting.readyAt()).isEqualTo(START);
        Duration delay = Duration.between(failedAt, waiting.nextAttemptAt());
        assertBackoffWithinBounds(delay, 1);
        assertThat(delay).isEqualTo(expectedDelay);

        Delivery nacked = delivery(id, 1);
        assertThat(nacked.ackState()).isEqualTo(AckState.NACKED);
        assertThat(nacked.error()).isEqualTo("RETRYABLE: boom");
        assertThat(nacked.finishedAt()).isEqualTo(failedAt);

        assertThat(listener.labels()).containsExactly("submitted", "claimed:1", "retryScheduled:RETRYABLE",
                "attemptFinished:NACKED");
        assertThat(listener.last("retryScheduled").delay()).isEqualTo(delay);
        assertThat(listener.last("retryScheduled").job()).isEqualTo(waiting);
        assertThat(dispatcher.depth(QUEUE)).as("not dispatched before the backoff elapsed").isZero();
    }

    @ParameterizedTest
    @EnumSource(value = FailureKind.class, names = {"RETRYABLE", "TIMEOUT", "WORKER_SHUTDOWN"})
    void retryableFailureKindsIncludingTimeoutAreRetried(FailureKind kind) {
        Lease lease = claim(submit(3).id(), "w1");

        assertThat(lifecycle.fail(lease, new TaskFailure(kind, "attempt 1"))).isEqualTo(Outcome.ACCEPTED);

        Job waiting = stored(lease.job().id());
        assertThat(waiting.state()).isEqualTo(JobState.RETRY_WAIT);
        assertThat(waiting.lastError()).isEqualTo(kind.name() + ": attempt 1");
        assertBackoffWithinBounds(Duration.between(START, waiting.nextAttemptAt()), 1);
        assertThat(listener.last("retryScheduled").detail()).isEqualTo(kind.name());
    }

    @Test
    void nonRetryableFailureFailsJobEvenWithAttemptsLeft() {
        Lease lease = claim(submit(5).id(), "w1");
        UUID id = lease.job().id();
        clock.advance(Duration.ofSeconds(1));

        assertThat(lifecycle.fail(lease, new TaskFailure(FailureKind.NON_RETRYABLE, "bad input")))
                .isEqualTo(Outcome.ACCEPTED);

        Job failed = stored(id);
        assertThat(failed.state()).isEqualTo(JobState.FAILED);
        assertThat(failed.attempts()).isEqualTo(1);
        assertThat(failed.finishedAt()).isEqualTo(START.plusSeconds(1));
        assertThat(failed.nextAttemptAt()).isNull();
        assertThat(failed.lastError()).isEqualTo("NON_RETRYABLE: bad input");
        assertThat(delivery(id, 1).ackState()).isEqualTo(AckState.NACKED);
        assertThat(delivery(id, 1).error()).isEqualTo("NON_RETRYABLE: bad input");
        assertThat(listener.labels()).containsExactly("submitted", "claimed:1", "attemptFinished:NACKED");

        clock.advance(Duration.ofHours(1));
        assertThat(reconciler.reconcileOnce().total()).isZero();
        assertThat(stored(id)).isEqualTo(failed);
    }

    @ParameterizedTest
    @EnumSource(value = FailureKind.class, names = {"RETRYABLE", "TIMEOUT"})
    void retryableFailureOnLastAttemptDeadLetters(FailureKind kind) {
        Lease lease = claim(submit(1).id(), "w1");
        UUID id = lease.job().id();
        clock.advance(Duration.ofSeconds(2));

        assertThat(lifecycle.fail(lease, new TaskFailure(kind, "boom"))).isEqualTo(Outcome.ACCEPTED);

        Job dead = stored(id);
        assertThat(dead.state()).isEqualTo(JobState.DEAD_LETTER);
        assertThat(dead.finishedAt()).isEqualTo(START.plusSeconds(2));
        assertThat(dead.nextAttemptAt()).isNull();
        assertThat(dead.lastError()).contains("attempts exhausted")
                .isEqualTo("attempts exhausted (1/1); last error: " + kind.name() + ": boom");
        assertThat(delivery(id, 1).ackState()).isEqualTo(AckState.NACKED);
        assertThat(delivery(id, 1).error()).isEqualTo(kind.name() + ": boom");
        assertThat(listener.count("retryScheduled")).isZero();
        assertThat(listener.last("attemptFinished").job().state()).isEqualTo(JobState.DEAD_LETTER);
    }

    @Test
    void nonRetryableFailureOnLastAttemptIsFailedNotDeadLettered() {
        Lease lease = claim(submit(1).id(), "w1");

        lifecycle.fail(lease, new TaskFailure(FailureKind.NON_RETRYABLE, "nope"));

        assertThat(stored(lease.job().id()).state()).isEqualTo(JobState.FAILED);
        assertThat(stored(lease.job().id()).lastError()).isEqualTo("NON_RETRYABLE: nope");
    }

    @Test
    void fullRetryCycleEndsInSuccess() {
        UUID id = submit(3).id();
        assertThat(drain(dispatcher)).containsExactly(id);

        Lease first = claim(id, "w1");
        assertThat(lifecycle.fail(first, BOOM)).isEqualTo(Outcome.ACCEPTED);
        Job wait1 = stored(id);
        assertBackoffWithinBounds(Duration.between(START, wait1.nextAttemptAt()), 1);

        clock.set(wait1.nextAttemptAt().minusMillis(1));
        assertThat(reconciler.reconcileOnce().retriesReleased()).isZero();
        clock.set(wait1.nextAttemptAt());
        assertThat(reconciler.reconcileOnce().retriesReleased()).isEqualTo(1);
        Job requeued = stored(id);
        assertThat(requeued.state()).isEqualTo(JobState.QUEUED);
        assertThat(requeued.version()).isEqualTo(4);
        assertThat(requeued.readyAt()).isEqualTo(wait1.nextAttemptAt());
        assertThat(requeued.dispatchedAt()).isEqualTo(wait1.nextAttemptAt());
        assertThat(requeued.nextAttemptAt()).isNull();
        assertThat(drain(dispatcher)).containsExactly(id);

        Lease second = claim(id, "w2");
        assertThat(second.attempt()).isEqualTo(2);
        assertThat(second.delivery().queuedAt()).isEqualTo(wait1.nextAttemptAt());
        Instant secondFailedAt = lifecycle.now();
        assertThat(lifecycle.fail(second, new TaskFailure(FailureKind.TIMEOUT, "slow"))).isEqualTo(Outcome.ACCEPTED);
        Job wait2 = stored(id);
        assertThat(wait2.state()).isEqualTo(JobState.RETRY_WAIT);
        assertBackoffWithinBounds(Duration.between(secondFailedAt, wait2.nextAttemptAt()), 2);

        clock.set(wait2.nextAttemptAt());
        assertThat(reconciler.reconcileOnce().retriesReleased()).isEqualTo(1);
        assertThat(drain(dispatcher)).containsExactly(id);
        Lease third = claim(id, "w3");
        assertThat(third.attempt()).isEqualTo(3);
        assertThat(lifecycle.complete(third, "{\"n\":3}")).isEqualTo(Outcome.ACCEPTED);

        Job done = stored(id);
        assertThat(done.state()).isEqualTo(JobState.SUCCEEDED);
        assertThat(done.attempts()).isEqualTo(3);
        assertThat(done.version()).isEqualTo(9);
        assertThat(done.lastError()).isNull();
        assertThat(done.result()).isEqualTo("{\"n\":3}");
        assertThat(repository.deliveries(id)).extracting(Delivery::attempt, Delivery::leaseOwner, Delivery::ackState)
                .containsExactly(
                        tuple(1, "w1", AckState.NACKED),
                        tuple(2, "w2", AckState.NACKED),
                        tuple(3, "w3", AckState.ACKED));
        assertThat(listener.count("claimed")).isEqualTo(3);
        assertThat(listener.count("retryScheduled")).isEqualTo(2);
        assertThat(listener.count("attemptFinished")).isEqualTo(3);
        assertThat(listener.labels()).filteredOn(l -> l.startsWith("recovered"))
                .containsExactly("recovered:RETRY_RELEASED", "recovered:RETRY_RELEASED");
    }

    // ---------------------------------------------------------------- cancellation

    @ParameterizedTest
    @EnumSource(value = JobState.class, names = {"PENDING", "QUEUED", "RETRY_WAIT", "RUNNING"})
    void cancelMovesActiveJobToCancelledImmediately(JobState from) {
        Job before = jobIn(from);
        clock.advance(Duration.ofSeconds(7));
        Instant cancelledAt = START.plusSeconds(7);

        Job cancelled = lifecycle.cancel(before.id());

        assertThat(cancelled.state()).isEqualTo(JobState.CANCELLED);
        assertThat(cancelled.version()).isEqualTo(before.version() + 1);
        assertThat(cancelled.finishedAt()).isEqualTo(cancelledAt);
        assertThat(cancelled.nextAttemptAt()).isNull();
        assertThat(cancelled.attempts()).isEqualTo(before.attempts());
        assertThat(stored(before.id())).isEqualTo(cancelled);
        assertThat(listener.last("cancelled").label()).isEqualTo("cancelled:" + from);
        assertThat(listener.last("cancelled").job()).isEqualTo(cancelled);

        // Nothing brings a cancelled job back, however much time passes.
        clock.advance(Duration.ofHours(1));
        reconciler.reconcileOnce();
        assertThat(stored(before.id()).state()).isEqualTo(JobState.CANCELLED);
        assertThat(stored(before.id()).version()).isEqualTo(cancelled.version());
    }

    @Test
    void cancelOfRunningJobLeavesDeliveryOpenUntilWorkerLearnsIt() {
        Lease lease = claim(submit(3).id(), "w1");
        UUID id = lease.job().id();

        Job cancelled = lifecycle.cancel(id);

        assertThat(cancelled.state()).isEqualTo(JobState.CANCELLED);
        assertThat(delivery(id, 1).ackState()).isEqualTo(AckState.LEASED);
        assertThat(lifecycle.heartbeat(lease, 10).status()).isEqualTo(HeartbeatResult.Status.CANCELLED);
    }

    @Test
    void cancelOfCancelledJobIsIdempotent() {
        UUID id = submit(3).id();
        Job cancelled = lifecycle.cancel(id);
        clock.advance(Duration.ofSeconds(5));

        assertThat(lifecycle.cancel(id)).isEqualTo(cancelled);
        assertThat(stored(id)).isEqualTo(cancelled);
        assertThat(listener.count("cancelled")).isEqualTo(1);
    }

    @ParameterizedTest
    @EnumSource(value = JobState.class, names = {"SUCCEEDED", "FAILED", "DEAD_LETTER"})
    void cancelOfFinishedJobIsAConflict(JobState state) {
        Job finished = jobIn(state);

        assertThatThrownBy(() -> lifecycle.cancel(finished.id()))
                .isInstanceOf(JobStateConflictException.class)
                .hasMessageContaining("already " + state)
                .satisfies(e -> assertThat(((JobStateConflictException) e).job()).isEqualTo(finished));
        assertThat(stored(finished.id())).isEqualTo(finished);
        assertThat(listener.count("cancelled")).isZero();
    }

    @Test
    void cancelOfUnknownJobIsNotFound() {
        UUID unknown = UUID.randomUUID();
        assertThatThrownBy(() -> lifecycle.cancel(unknown))
                .isInstanceOf(JobNotFoundException.class)
                .hasMessageContaining(unknown.toString());
    }

    // ---------------------------------------------------------------- stale workers

    @Test
    void zombieWorkerCannotOverwriteTheAttemptThatReplacedIt() {
        UUID id = submit(3).id();
        drain(dispatcher);
        Lease zombie = claim(id, "w1");
        clock.advance(Duration.ofSeconds(10));
        Lease zombieRenewed = renewed(zombie, 30); // deadline START + 40 s
        assertThat(zombieRenewed.deadline()).isEqualTo(START.plusSeconds(40));

        // w1 stalls; its lease runs out and the reconciler reclaims it after the grace period.
        clock.set(zombieRenewed.deadline().plus(RECONCILER.leaseGrace()).plusMillis(1));
        assertThat(reconciler.reconcileOnce().leasesExpired()).isEqualTo(1);
        Job waiting = stored(id);
        assertThat(waiting.state()).isEqualTo(JobState.RETRY_WAIT);
        assertThat(waiting.lastError()).startsWith("LEASE_EXPIRED: lease held by w1 expired at "
                + zombieRenewed.deadline());
        assertThat(delivery(id, 1).ackState()).isEqualTo(AckState.EXPIRED);

        clock.set(waiting.nextAttemptAt());
        assertThat(reconciler.reconcileOnce().retriesReleased()).isEqualTo(1);
        assertThat(drain(dispatcher)).containsExactly(id);
        Lease replacement = claim(id, "w2");
        assertThat(replacement.attempt()).isEqualTo(2);
        Delivery replacementDelivery = delivery(id, 2);
        Delivery zombieDelivery = delivery(id, 1);

        // The zombie wakes up: every report is refused and nothing it sends reaches the new attempt.
        assertThat(lifecycle.heartbeat(zombieRenewed, 99).status()).isEqualTo(HeartbeatResult.Status.LOST);
        assertThat(lifecycle.complete(zombieRenewed, "{\"from\":\"w1\"}")).isEqualTo(Outcome.LEASE_LOST);
        assertThat(lifecycle.fail(zombieRenewed, BOOM)).isEqualTo(Outcome.LEASE_LOST);
        assertThat(stored(id)).isEqualTo(replacement.job());
        assertThat(delivery(id, 1)).isEqualTo(zombieDelivery);
        assertThat(delivery(id, 2)).isEqualTo(replacementDelivery);

        assertThat(lifecycle.complete(replacement, "{\"from\":\"w2\"}")).isEqualTo(Outcome.ACCEPTED);
        Job done = stored(id);
        assertThat(done.state()).isEqualTo(JobState.SUCCEEDED);
        assertThat(done.result()).isEqualTo("{\"from\":\"w2\"}");
        assertThat(repository.deliveries(id)).extracting(Delivery::leaseOwner, Delivery::ackState)
                .containsExactly(tuple("w1", AckState.EXPIRED), tuple("w2", AckState.ACKED));

        assertThat(listener.count("leaseExpired")).isEqualTo(1);
        assertThat(listener.last("leaseExpired").job().attempts()).isEqualTo(1);
        assertThat(listener.last("leaseExpired").delivery().leaseOwner()).isEqualTo("w1");
        assertThat(listener.named("retryScheduled")).extracting(Event::detail).containsExactly("LEASE_EXPIRED");
        assertThat(listener.named("attemptFinished")).extracting(e -> e.delivery().ackState())
                .containsExactly(AckState.EXPIRED, AckState.ACKED);
        assertThat(listener.count("claimed")).isEqualTo(2);
    }

    @Test
    void expiryIsRefusedWhenTheLeaseWasRenewedAfterTheScan() {
        Lease lease = claim(submit(3).id(), "w1");
        UUID id = lease.job().id();
        clock.advance(LEASE.plusSeconds(1));
        Delivery scanned = delivery(id, 1);
        Instant expiredBefore = lifecycle.now();

        Lease renewed = renewed(lease, 5);

        assertThat(lifecycle.expireLease(scanned, expiredBefore)).isFalse();
        assertThat(stored(id)).isEqualTo(lease.job());
        assertThat(delivery(id, 1)).isEqualTo(renewed.delivery());
        assertThat(listener.count("leaseExpired")).isZero();
    }

    // ---------------------------------------------------------------- recovery operations and listener

    @Test
    void recoveryOperationsAreCompareAndSetAndNotifyTheListener() {
        UUID pendingId = crashSubmit(Point.AFTER_PERSIST, 3);
        UUID queuedId = crashSubmit(Point.AFTER_ENQUEUE_BEFORE_DISPATCH, 3);
        Job pending = stored(pendingId);
        Job queued = stored(queuedId);
        assertThat(dispatcher.depth(QUEUE)).isZero();
        clock.advance(Duration.ofSeconds(30));
        Instant now = lifecycle.now();

        assertThat(lifecycle.enqueuePending(pending)).isTrue();
        assertThat(lifecycle.enqueuePending(pending)).as("stale snapshot").isFalse();
        Job enqueued = stored(pendingId);
        assertThat(enqueued.state()).isEqualTo(JobState.QUEUED);
        assertThat(enqueued.version()).isEqualTo(1);
        assertThat(enqueued.readyAt()).isEqualTo(pending.createdAt());
        assertThat(enqueued.dispatchedAt()).isEqualTo(now);

        lifecycle.redispatch(queued);
        Job redispatched = stored(queuedId);
        assertThat(redispatched.version()).isEqualTo(queued.version());
        assertThat(redispatched.dispatchedAt()).isEqualTo(now);
        assertThat(redispatched.readyAt()).isEqualTo(queued.readyAt());
        assertThat(drain(dispatcher)).containsExactly(pendingId, queuedId);

        Lease lease = claim(queuedId, "w1");
        lifecycle.fail(lease, BOOM);
        Job waiting = stored(queuedId);
        lifecycle.cancel(queuedId);
        assertThat(lifecycle.releaseRetry(waiting)).as("cancelled meanwhile").isFalse();
        assertThat(stored(queuedId).state()).isEqualTo(JobState.CANCELLED);
        assertThat(dispatcher.depth(QUEUE)).isZero();

        assertThat(listener.labels()).filteredOn(l -> l.startsWith("recovered"))
                .containsExactly("recovered:PENDING_ENQUEUED", "recovered:QUEUED_REDISPATCHED");
    }

    @Test
    void listenerSeesEveryEventInOrder() {
        UUID id = submit(3).id();
        drain(dispatcher);
        Lease first = claim(id, "w1");
        lifecycle.fail(first, BOOM);
        clock.set(stored(id).nextAttemptAt());
        reconciler.reconcileOnce();
        claim(id, "w2");
        clock.advance(LEASE.plus(RECONCILER.leaseGrace()).plusMillis(1));
        reconciler.reconcileOnce();
        lifecycle.cancel(id);

        assertThat(listener.labels()).containsExactly(
                "submitted",
                "claimed:1",
                "retryScheduled:RETRYABLE",
                "attemptFinished:NACKED",
                "recovered:RETRY_RELEASED",
                "claimed:2",
                "leaseExpired:2",
                "retryScheduled:LEASE_EXPIRED",
                "attemptFinished:EXPIRED",
                "cancelled:RETRY_WAIT");
        assertThat(listener.named("claimed")).extracting(e -> e.delivery().leaseOwner()).containsExactly("w1", "w2");
        assertThat(listener.last("leaseExpired").job().state()).isEqualTo(JobState.RUNNING);
        assertThat(listener.last("attemptFinished").job().state()).isEqualTo(JobState.RETRY_WAIT);
    }

    @Test
    void rejectsNonPositiveLeaseDuration() {
        assertThatThrownBy(() -> new JobLifecycle(repository, dispatcher, retryPolicy(), clock, Duration.ZERO,
                listener, checkpoints)).isInstanceOf(IllegalArgumentException.class);
    }

    // ---------------------------------------------------------------- concurrency

    @Test
    void concurrentClaimsOfOneJobHaveExactlyOneWinner() throws Exception {
        int threads = 8;
        int rounds = 50;
        executor = Executors.newFixedThreadPool(threads);
        for (int round = 0; round < rounds; round++) {
            UUID id = submit(3).id();
            CyclicBarrier barrier = new CyclicBarrier(threads);
            List<Future<Optional<Lease>>> claims = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                String worker = "w" + t;
                claims.add(executor.submit(() -> {
                    barrier.await(5, SECONDS);
                    return lifecycle.claim(id, worker);
                }));
            }
            List<Lease> winners = new ArrayList<>();
            for (Future<Optional<Lease>> claim : claims) claim.get(10, SECONDS).ifPresent(winners::add);

            assertThat(winners).as("round %d", round).hasSize(1);
            assertThat(stored(id)).isEqualTo(winners.get(0).job());
            assertThat(stored(id).attempts()).isEqualTo(1);
            assertThat(repository.deliveries(id)).containsExactly(winners.get(0).delivery());
        }
        assertThat(listener.count("claimed")).isEqualTo(rounds);
    }

    @Test
    void cancelRacingWithCompleteHasOneConsistentOutcome() throws Exception {
        int rounds = 200;
        executor = Executors.newFixedThreadPool(2);
        int succeeded = 0;
        int cancelled = 0;
        for (int round = 0; round < rounds; round++) {
            Lease lease = claim(submit(3).id(), "w1");
            UUID id = lease.job().id();
            CyclicBarrier barrier = new CyclicBarrier(2);
            Future<Outcome> completion = executor.submit(() -> {
                barrier.await(5, SECONDS);
                return lifecycle.complete(lease, "{}");
            });
            Future<Object> cancellation = executor.submit(() -> {
                barrier.await(5, SECONDS);
                try {
                    return lifecycle.cancel(id);
                } catch (JobStateConflictException e) {
                    return e;
                }
            });
            Outcome outcome = completion.get(10, SECONDS);
            Object cancelResult = cancellation.get(10, SECONDS);
            Job last = stored(id);

            if (outcome == Outcome.ACCEPTED) {
                succeeded++;
                assertThat(last.state()).as("round %d", round).isEqualTo(JobState.SUCCEEDED);
                assertThat(cancelResult).isInstanceOf(JobStateConflictException.class);
                assertThat(delivery(id, 1).ackState()).isEqualTo(AckState.ACKED);
            } else {
                cancelled++;
                assertThat(outcome).as("round %d", round).isEqualTo(Outcome.CANCELLED);
                assertThat(last.state()).isEqualTo(JobState.CANCELLED);
                assertThat(last.result()).isNull();
                assertThat(cancelResult).isEqualTo(last);
                assertThat(delivery(id, 1).ackState()).isNotIn(AckState.ACKED, AckState.NACKED);
                lifecycle.acknowledgeCancel(lease);
                assertThat(delivery(id, 1).ackState()).isEqualTo(AckState.CANCELLED);
            }
        }
        assertThat(succeeded + cancelled).isEqualTo(rounds);
        assertThat(listener.count("cancelled")).isEqualTo(cancelled);
        assertThat(listener.count("attemptFinished")).isEqualTo(succeeded);
    }

    @Test
    void heartbeatRacingWithLeaseExpiryHasExactlyOneWinner() throws Exception {
        int rounds = 200;
        executor = Executors.newFixedThreadPool(2);
        for (int round = 0; round < rounds; round++) {
            Lease lease = claim(submit(3).id(), "w1");
            UUID id = lease.job().id();
            clock.advance(LEASE.plusSeconds(1));
            Delivery scanned = delivery(id, 1);
            Instant expiredBefore = lifecycle.now();
            CyclicBarrier barrier = new CyclicBarrier(2);
            Future<HeartbeatResult> beat = executor.submit(() -> {
                barrier.await(5, SECONDS);
                return lifecycle.heartbeat(lease, 10);
            });
            Future<Boolean> expiry = executor.submit(() -> {
                barrier.await(5, SECONDS);
                return lifecycle.expireLease(scanned, expiredBefore);
            });
            HeartbeatResult heartbeat = beat.get(10, SECONDS);
            boolean expired = expiry.get(10, SECONDS);

            if (expired) {
                assertThat(heartbeat.status()).as("round %d", round).isEqualTo(HeartbeatResult.Status.LOST);
                assertThat(stored(id).state()).isEqualTo(JobState.RETRY_WAIT);
                assertThat(delivery(id, 1).ackState()).isEqualTo(AckState.EXPIRED);
            } else {
                assertThat(heartbeat.status()).as("round %d", round).isEqualTo(HeartbeatResult.Status.RENEWED);
                assertThat(stored(id)).isEqualTo(lease.job());
                assertThat(delivery(id, 1)).isEqualTo(heartbeat.lease().delivery());
                assertThat(delivery(id, 1).leaseDeadline()).isEqualTo(expiredBefore.plus(LEASE));
            }
        }
    }

    // ---------------------------------------------------------------- helpers

    static RetryPolicy retryPolicy() {
        return new RetryPolicy(Duration.ofSeconds(1), 2.0, Duration.ofMinutes(1), 0.5, new Random(SEED));
    }

    /** Independent statement of the policy above: base = min(60 s, 1 s * 2^(n-1)), delay in [base / 2, base]. */
    static void assertBackoffWithinBounds(Duration delay, int failedAttempt) {
        long base = Math.min(60_000L, 1_000L << (failedAttempt - 1));
        assertThat(delay.toMillis()).as("backoff after failed attempt %d", failedAttempt).isBetween(base / 2, base);
        assertThat(delay.toNanos() % 1_000_000).as("whole milliseconds").isZero();
    }

    static SubmitCommand command(int maxAttempts, String idempotencyKey) {
        return new SubmitCommand(QUEUE, "sleep", "{\"durationMs\":1}", maxAttempts, Duration.ofSeconds(30),
                idempotencyKey);
    }

    static List<UUID> drain(InMemoryDispatcher queue) {
        List<UUID> ids = new ArrayList<>();
        try {
            for (Optional<UUID> id = queue.poll(QUEUE, Duration.ZERO); id.isPresent();
                 id = queue.poll(QUEUE, Duration.ZERO)) {
                ids.add(id.get());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
        return ids;
    }

    private Job submit(int maxAttempts) {
        return lifecycle.submit(command(maxAttempts, null)).job();
    }

    private UUID crashSubmit(Point point, int maxAttempts) {
        checkpoints.crashAt(point);
        try {
            assertThatThrownBy(() -> lifecycle.submit(command(maxAttempts, null))).isInstanceOf(SimulatedCrash.class);
        } finally {
            checkpoints.clear();
        }
        return checkpoints.lastReached();
    }

    private Lease claim(UUID id, String worker) {
        return lifecycle.claim(id, worker).orElseThrow(() -> new AssertionError("claim of " + id + " failed"));
    }

    private Lease renewed(Lease lease, Integer progress) {
        HeartbeatResult result = lifecycle.heartbeat(lease, progress);
        assertThat(result.status()).isEqualTo(HeartbeatResult.Status.RENEWED);
        return result.lease();
    }

    private Job stored(UUID id) {
        return repository.find(id).orElseThrow();
    }

    private Delivery delivery(UUID id, int attempt) {
        return repository.deliveries(id).get(attempt - 1);
    }

    /** Drives a fresh job into {@code state} through the public operations only. */
    private Job jobIn(JobState state) {
        Job job = switch (state) {
            case PENDING -> stored(crashSubmit(Point.AFTER_PERSIST, 3));
            case QUEUED -> submit(3);
            case RUNNING -> claim(submit(3).id(), "w1").job();
            case RETRY_WAIT -> finishWith(claim(submit(3).id(), "w1"), l -> lifecycle.fail(l, BOOM));
            case SUCCEEDED -> finishWith(claim(submit(3).id(), "w1"), l -> lifecycle.complete(l, "{}"));
            case FAILED -> finishWith(claim(submit(3).id(), "w1"),
                    l -> lifecycle.fail(l, new TaskFailure(FailureKind.NON_RETRYABLE, "bad")));
            case DEAD_LETTER -> finishWith(claim(submit(1).id(), "w1"), l -> lifecycle.fail(l, BOOM));
            case CANCELLED -> lifecycle.cancel(submit(3).id());
        };
        assertThat(job.state()).isEqualTo(state);
        assertThat(stored(job.id())).isEqualTo(job);
        return job;
    }

    private Job finishWith(Lease lease, Consumer<Lease> report) {
        report.accept(lease);
        return stored(lease.job().id());
    }

    // ---------------------------------------------------------------- test doubles

    /** One listener callback, with a compact label for order assertions. */
    record Event(String name, String detail, Job job, Delivery delivery, Duration delay) {
        String label() {
            return detail == null ? name : name + ":" + detail;
        }
    }

    /** Records every lifecycle event; thread-safe. */
    static final class RecordingListener implements LifecycleListener {
        private final List<Event> events = new CopyOnWriteArrayList<>();

        @Override
        public void onSubmitted(Job job) {
            events.add(new Event("submitted", null, job, null, null));
        }

        @Override
        public void onClaimed(Job running, Delivery delivery) {
            events.add(new Event("claimed", String.valueOf(delivery.attempt()), running, delivery, null));
        }

        @Override
        public void onAttemptFinished(Job next, Delivery closed) {
            events.add(new Event("attemptFinished", closed.ackState().name(), next, closed, null));
        }

        @Override
        public void onRetryScheduled(Job next, FailureKind reason, Duration delay) {
            events.add(new Event("retryScheduled", reason.name(), next, null, delay));
        }

        @Override
        public void onLeaseExpired(Job running, Delivery expired) {
            events.add(new Event("leaseExpired", String.valueOf(expired.attempt()), running, expired, null));
        }

        @Override
        public void onCancelled(Job cancelled, JobState from) {
            events.add(new Event("cancelled", from.name(), cancelled, null, null));
        }

        @Override
        public void onRecovered(Recovery kind, Job job) {
            events.add(new Event("recovered", kind.name(), job, null, null));
        }

        List<String> labels() {
            return events.stream().map(Event::label).toList();
        }

        List<Event> named(String name) {
            return events.stream().filter(e -> e.name().equals(name)).toList();
        }

        long count(String name) {
            return named(name).size();
        }

        Event last(String name) {
            List<Event> matching = named(name);
            assertThat(matching).as("events named " + name).isNotEmpty();
            return matching.get(matching.size() - 1);
        }
    }

    /** Checkpoints that run a scripted action (typically a simulated crash) at a chosen point. */
    static final class ScriptedCheckpoints implements Checkpoints {
        private final Map<Point, Consumer<UUID>> actions = new ConcurrentHashMap<>();
        private final List<UUID> reached = new CopyOnWriteArrayList<>();

        void crashAt(Point point) {
            on(point, id -> {
                throw new SimulatedCrash(point, id);
            });
        }

        void on(Point point, Consumer<UUID> action) {
            actions.put(point, action);
        }

        void clear() {
            actions.clear();
        }

        UUID lastReached() {
            return reached.get(reached.size() - 1);
        }

        @Override
        public void reached(Point point, UUID jobId) {
            Consumer<UUID> action = actions.get(point);
            if (action != null) {
                reached.add(jobId);
                action.accept(jobId);
            }
        }
    }

    /** The process "dies" at a checkpoint. */
    static final class SimulatedCrash extends RuntimeException {
        SimulatedCrash(Point point, UUID jobId) {
            super("simulated crash at " + point + " for job " + jobId);
        }
    }
}
