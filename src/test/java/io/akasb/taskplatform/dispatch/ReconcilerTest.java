package io.akasb.taskplatform.dispatch;

import static io.akasb.taskplatform.dispatch.JobLifecycleTest.LEASE;
import static io.akasb.taskplatform.dispatch.JobLifecycleTest.QUEUE;
import static io.akasb.taskplatform.dispatch.JobLifecycleTest.START;
import static io.akasb.taskplatform.dispatch.JobLifecycleTest.assertBackoffWithinBounds;
import static io.akasb.taskplatform.dispatch.JobLifecycleTest.command;
import static io.akasb.taskplatform.dispatch.JobLifecycleTest.drain;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.akasb.taskplatform.dispatch.Checkpoints.Point;
import io.akasb.taskplatform.dispatch.JobLifecycleTest.RecordingListener;
import io.akasb.taskplatform.dispatch.JobLifecycleTest.ScriptedCheckpoints;
import io.akasb.taskplatform.dispatch.JobLifecycleTest.SimulatedCrash;
import io.akasb.taskplatform.dispatch.Reconciler.Report;
import io.akasb.taskplatform.domain.AckState;
import io.akasb.taskplatform.domain.Delivery;
import io.akasb.taskplatform.domain.FailureKind;
import io.akasb.taskplatform.domain.Job;
import io.akasb.taskplatform.domain.JobState;
import io.akasb.taskplatform.domain.Lease;
import io.akasb.taskplatform.domain.TaskFailure;
import io.akasb.taskplatform.persistence.InMemoryJobRepository;
import io.akasb.taskplatform.persistence.InsertResult;
import io.akasb.taskplatform.persistence.JobRepository;
import io.akasb.taskplatform.persistence.QueueStats;
import io.akasb.taskplatform.support.MutableClock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * {@link Reconciler} passes on the in-memory repository: recovery from a crash at either submission checkpoint,
 * lease expiry with grace and racing renewals, retry release, startup recovery for durable and non-durable
 * dispatchers, batch limits, report counts and concurrent reconcilers. Time only moves through {@link MutableClock}.
 */
class ReconcilerTest {
    private static final Duration LEASE_GRACE = Duration.ofSeconds(5);
    private static final Duration PENDING_GRACE = Duration.ofSeconds(10);
    private static final Duration REDISPATCH_AFTER = Duration.ofSeconds(20);
    private static final Duration MS = Duration.ofMillis(1);
    private static final Report NOTHING = new Report(0, 0, 0, 0);

    private MutableClock clock;
    private JobRepository repository;
    private JobDispatcher dispatcher;
    private InMemoryDispatcher queue;
    private RecordingListener listener;
    private ScriptedCheckpoints checkpoints;
    private JobLifecycle lifecycle;
    private Reconciler reconciler;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(START);
        queue = new InMemoryDispatcher();
        build(new InMemoryJobRepository(), queue, 100);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (executor != null) {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    /** (Re)creates the lifecycle and reconciler; the clock is kept, like the database across a restart. */
    private void build(JobRepository repo, JobDispatcher jobDispatcher, int batchSize) {
        repository = repo;
        dispatcher = jobDispatcher;
        listener = new RecordingListener();
        checkpoints = new ScriptedCheckpoints();
        lifecycle = new JobLifecycle(repository, dispatcher, JobLifecycleTest.retryPolicy(), clock, LEASE, listener,
                checkpoints);
        reconciler = new Reconciler(repository, lifecycle, dispatcher, settings(batchSize));
    }

    private static Reconciler.Settings settings(int batchSize) {
        return new Reconciler.Settings(LEASE_GRACE, PENDING_GRACE, REDISPATCH_AFTER, batchSize);
    }

    // ---------------------------------------------------------------- crash recovery

    @Test
    void crashAfterPersistLeavesPendingJobThatIsEnqueuedAfterPendingGrace() {
        UUID id = crashSubmit(Point.AFTER_PERSIST);
        Job pending = stored(id);
        assertThat(pending.state()).isEqualTo(JobState.PENDING);
        assertThat(pending.version()).isZero();
        assertThat(pending.readyAt()).isNull();
        assertThat(pending.dispatchedAt()).isNull();
        assertThat(queue.depth(QUEUE)).isZero();

        assertThat(reconciler.reconcileOnce()).isEqualTo(NOTHING);
        clock.advance(PENDING_GRACE.minus(MS));
        assertThat(reconciler.reconcileOnce()).isEqualTo(NOTHING);
        assertThat(stored(id)).isEqualTo(pending);
        assertThat(queue.depth(QUEUE)).isZero();

        clock.advance(MS.multipliedBy(2));
        Instant recoveredAt = lifecycle.now();
        assertThat(reconciler.reconcileOnce()).isEqualTo(new Report(0, 0, 1, 0));

        Job queued = stored(id);
        assertThat(queued.state()).isEqualTo(JobState.QUEUED);
        assertThat(queued.version()).isEqualTo(1);
        assertThat(queued.readyAt()).isEqualTo(pending.createdAt());
        assertThat(queued.dispatchedAt()).isEqualTo(recoveredAt);
        assertThat(drain(queue)).containsExactly(id);
        assertThat(listener.last("recovered").label()).isEqualTo("recovered:PENDING_ENQUEUED");
        assertThat(listener.last("recovered").job()).isEqualTo(queued);
        assertThat(reconciler.reconcileOnce()).isEqualTo(NOTHING);

        // The recovered job runs normally; its queue latency includes the time spent in limbo.
        Lease lease = claim(id, "w1");
        assertThat(lease.delivery().queuedAt()).isEqualTo(START);
    }

    @Test
    void crashBeforeDispatchLeavesQueuedJobThatIsRedispatchedAfterRedispatchAfter() {
        UUID id = crashSubmit(Point.AFTER_ENQUEUE_BEFORE_DISPATCH);
        Job queued = stored(id);
        assertThat(queued.state()).isEqualTo(JobState.QUEUED);
        assertThat(queued.version()).isEqualTo(1);
        assertThat(queued.dispatchedAt()).isEqualTo(START);
        assertThat(queue.depth(QUEUE)).as("never handed to the dispatcher").isZero();

        assertThat(reconciler.reconcileOnce()).isEqualTo(NOTHING);
        clock.advance(REDISPATCH_AFTER.minus(MS));
        assertThat(reconciler.reconcileOnce()).isEqualTo(NOTHING);
        assertThat(queue.depth(QUEUE)).isZero();

        clock.advance(MS.multipliedBy(2));
        Instant redispatchedAt = lifecycle.now();
        assertThat(reconciler.reconcileOnce()).isEqualTo(new Report(0, 0, 0, 1));

        Job redispatched = stored(id);
        assertThat(redispatched.state()).isEqualTo(JobState.QUEUED);
        assertThat(redispatched.version()).as("re-dispatch does not bump the version").isEqualTo(1);
        assertThat(redispatched.dispatchedAt()).isEqualTo(redispatchedAt);
        assertThat(redispatched.readyAt()).isEqualTo(START);
        assertThat(drain(queue)).containsExactly(id);
        assertThat(listener.last("recovered").label()).isEqualTo("recovered:QUEUED_REDISPATCHED");
        assertThat(reconciler.reconcileOnce()).isEqualTo(NOTHING);
    }

    @Test
    void crashBeforeDispatchIsRecoveredImmediatelyAtStartup() {
        UUID id = crashSubmit(Point.AFTER_ENQUEUE_BEFORE_DISPATCH);

        assertThat(reconciler.recoverOnStartup()).isEqualTo(new Report(0, 0, 0, 1));

        assertThat(drain(queue)).containsExactly(id);
        assertThat(stored(id).version()).isEqualTo(1);
    }

    @Test
    void restartWithNonDurableDispatcherRedispatchesEveryQueuedJob() {
        Job delivered = lifecycle.submit(command(3, null)).job(); // its message dies with the old process
        UUID lost = crashSubmit(Point.AFTER_ENQUEUE_BEFORE_DISPATCH);
        clock.advance(Duration.ofSeconds(1));
        assertThat(reconciler.reconcileOnce()).as("a periodic pass would still wait").isEqualTo(NOTHING);

        InMemoryDispatcher fresh = new InMemoryDispatcher();
        build(repository, fresh, 100);
        assertThat(reconciler.recoverOnStartup()).isEqualTo(new Report(0, 0, 0, 2));

        assertThat(drain(fresh)).containsExactlyInAnyOrder(delivered.id(), lost);
        assertThat(stored(delivered.id()).dispatchedAt()).isEqualTo(lifecycle.now());
        assertThat(stored(lost).dispatchedAt()).isEqualTo(lifecycle.now());
        assertThat(stored(delivered.id()).version()).isEqualTo(1);
    }

    @Test
    void durableDispatcherAtStartupOnlyRedispatchesOldQueuedJobs() {
        RecordingDispatcher durable = new RecordingDispatcher(true);
        build(repository, durable, 100);
        Job old = lifecycle.submit(command(3, null)).job();
        clock.advance(REDISPATCH_AFTER);
        Job recent = lifecycle.submit(command(3, null)).job();
        clock.advance(MS);
        assertThat(durable.dispatched()).containsExactly(old.id(), recent.id());

        assertThat(reconciler.recoverOnStartup()).isEqualTo(new Report(0, 0, 0, 1));

        assertThat(durable.dispatched()).containsExactly(old.id(), recent.id(), old.id());
        assertThat(stored(old.id()).dispatchedAt()).isEqualTo(lifecycle.now());
        assertThat(stored(recent.id()).dispatchedAt()).isEqualTo(START.plus(REDISPATCH_AFTER));

        // The same state behind a non-durable dispatcher: every QUEUED job is sent again.
        RecordingDispatcher nonDurable = new RecordingDispatcher(false);
        build(repository, nonDurable, 100);
        assertThat(reconciler.recoverOnStartup()).isEqualTo(new Report(0, 0, 0, 2));
        assertThat(nonDurable.dispatched()).containsExactlyInAnyOrder(old.id(), recent.id());
    }

    @Test
    void dispatchFailureDuringSubmitIsRepairedByRedispatch() {
        RecordingDispatcher broker = new RecordingDispatcher(true);
        broker.failNext();
        build(repository, broker, 100);

        SubmitResult result = lifecycle.submit(command(3, null));
        UUID id = result.job().id();
        assertThat(result.created()).isTrue();
        assertThat(stored(id).state()).isEqualTo(JobState.QUEUED);
        assertThat(broker.dispatched()).isEmpty();

        clock.advance(REDISPATCH_AFTER.plus(MS));
        assertThat(reconciler.reconcileOnce()).isEqualTo(new Report(0, 0, 0, 1));
        assertThat(broker.dispatched()).containsExactly(id);
    }

    @Test
    void pendingJobCancelledBeforeRecoveryIsLeftAlone() {
        UUID id = crashSubmit(Point.AFTER_PERSIST);
        Job cancelled = lifecycle.cancel(id);
        clock.advance(PENDING_GRACE.multipliedBy(10));

        assertThat(reconciler.reconcileOnce()).isEqualTo(NOTHING);
        assertThat(stored(id)).isEqualTo(cancelled);
        assertThat(queue.depth(QUEUE)).isZero();
    }

    // ---------------------------------------------------------------- leases and retries

    @Test
    void expiredLeaseIsRetriedAndReleasedToQueuedOnlyWhenDue() {
        UUID id = lifecycle.submit(command(3, null)).job().id();
        drain(queue);
        Lease lease = claim(id, "w1");

        clock.advance(LEASE.plusSeconds(1));
        assertThat(reconciler.reconcileOnce()).as("past the deadline but within the grace").isEqualTo(NOTHING);
        assertThat(stored(id).state()).isEqualTo(JobState.RUNNING);

        clock.set(lease.deadline().plus(LEASE_GRACE).plus(MS));
        Instant reclaimedAt = lifecycle.now();
        assertThat(reconciler.reconcileOnce()).isEqualTo(new Report(1, 0, 0, 0));

        String reason = "LEASE_EXPIRED: lease held by w1 expired at " + lease.deadline();
        Job waiting = stored(id);
        assertThat(waiting.state()).isEqualTo(JobState.RETRY_WAIT);
        assertThat(waiting.attempts()).isEqualTo(1);
        assertThat(waiting.version()).isEqualTo(3);
        assertThat(waiting.lastError()).isEqualTo(reason);
        assertBackoffWithinBounds(Duration.between(reclaimedAt, waiting.nextAttemptAt()), 1);
        Delivery expired = repository.deliveries(id).get(0);
        assertThat(expired.ackState()).isEqualTo(AckState.EXPIRED);
        assertThat(expired.finishedAt()).isEqualTo(reclaimedAt);
        assertThat(expired.error()).isEqualTo(reason);
        assertThat(expired.leaseDeadline()).isEqualTo(lease.deadline());
        assertThat(queue.depth(QUEUE)).isZero();

        assertThat(reconciler.reconcileOnce()).isEqualTo(NOTHING);
        clock.set(waiting.nextAttemptAt().minus(MS));
        assertThat(reconciler.reconcileOnce()).isEqualTo(NOTHING);
        assertThat(stored(id)).isEqualTo(waiting);
        assertThat(queue.depth(QUEUE)).isZero();

        clock.set(waiting.nextAttemptAt());
        assertThat(reconciler.reconcileOnce()).isEqualTo(new Report(0, 1, 0, 0));
        Job released = stored(id);
        assertThat(released.state()).isEqualTo(JobState.QUEUED);
        assertThat(released.version()).isEqualTo(4);
        assertThat(released.readyAt()).isEqualTo(waiting.nextAttemptAt());
        assertThat(released.dispatchedAt()).isEqualTo(waiting.nextAttemptAt());
        assertThat(released.nextAttemptAt()).isNull();
        assertThat(drain(queue)).containsExactly(id);

        Lease second = claim(id, "w2");
        assertThat(second.attempt()).isEqualTo(2);
        assertThat(second.delivery().queuedAt()).isEqualTo(waiting.nextAttemptAt());
    }

    @Test
    void leaseRenewedJustBeforeTheScanIsNotExpired() {
        Lease lease = claim(lifecycle.submit(command(3, null)).job().id(), "w1");
        UUID id = lease.job().id();
        clock.advance(Duration.ofSeconds(34));
        HeartbeatResult beat = lifecycle.heartbeat(lease, 20);
        assertThat(beat.status()).isEqualTo(HeartbeatResult.Status.RENEWED);

        clock.set(lease.deadline().plus(LEASE_GRACE).plusSeconds(1)); // the original lease would be reclaimable
        assertThat(reconciler.reconcileOnce()).isEqualTo(NOTHING);
        assertThat(stored(id)).isEqualTo(lease.job());
        assertThat(repository.deliveries(id)).containsExactly(beat.lease().delivery());

        clock.set(beat.lease().deadline().plus(LEASE_GRACE).minus(MS));
        assertThat(reconciler.reconcileOnce()).isEqualTo(NOTHING);
        clock.advance(MS.multipliedBy(2));
        assertThat(reconciler.reconcileOnce()).isEqualTo(new Report(1, 0, 0, 0));
        assertThat(stored(id).state()).isEqualTo(JobState.RETRY_WAIT);
    }

    @Test
    void renewalThatRacesWithTheScanWinsOverExpiry() {
        RacingRepository racing = new RacingRepository(new InMemoryJobRepository());
        build(racing, queue, 100);
        Lease lease = claim(lifecycle.submit(command(3, null)).job().id(), "w1");
        UUID id = lease.job().id();
        clock.advance(LEASE.plus(LEASE_GRACE).plusSeconds(1));
        List<HeartbeatResult> beats = new ArrayList<>();
        // The scan has already listed the lease as expired when the worker's heartbeat lands.
        racing.afterFindExpiredLeases(() -> beats.add(lifecycle.heartbeat(lease, 60)));

        assertThat(reconciler.reconcileOnce()).isEqualTo(NOTHING);

        assertThat(beats).extracting(HeartbeatResult::status).containsExactly(HeartbeatResult.Status.RENEWED);
        assertThat(stored(id)).isEqualTo(lease.job());
        Delivery open = repository.deliveries(id).get(0);
        assertThat(open.ackState()).isEqualTo(AckState.LEASED);
        assertThat(open.leaseDeadline()).isEqualTo(lifecycle.now().plus(LEASE));
        assertThat(open.progress()).isEqualTo(60);
        assertThat(listener.count("leaseExpired")).isZero();
        assertThat(listener.count("attemptFinished")).isZero();
    }

    @Test
    void leaseExpiryOnTheLastAttemptDeadLetters() {
        Lease lease = claim(lifecycle.submit(command(1, null)).job().id(), "w1");
        UUID id = lease.job().id();
        clock.set(lease.deadline().plus(LEASE_GRACE).plus(MS));
        Instant reclaimedAt = lifecycle.now();

        assertThat(reconciler.reconcileOnce()).isEqualTo(new Report(1, 0, 0, 0));

        Job dead = stored(id);
        assertThat(dead.state()).isEqualTo(JobState.DEAD_LETTER);
        assertThat(dead.finishedAt()).isEqualTo(reclaimedAt);
        assertThat(dead.nextAttemptAt()).isNull();
        assertThat(dead.lastError()).isEqualTo("attempts exhausted (1/1); last error: LEASE_EXPIRED: lease held by w1"
                + " expired at " + lease.deadline());
        assertThat(repository.deliveries(id)).extracting(Delivery::ackState).containsExactly(AckState.EXPIRED);
        assertThat(listener.count("leaseExpired")).isEqualTo(1);
        assertThat(listener.count("retryScheduled")).isZero();
        assertThat(listener.last("attemptFinished").job()).isEqualTo(dead);

        clock.advance(Duration.ofHours(1));
        assertThat(reconciler.reconcileOnce()).isEqualTo(NOTHING);
        assertThat(stored(id)).isEqualTo(dead);
    }

    @Test
    void expiredLeaseOfCancelledJobOnlyClosesTheDelivery() {
        Lease lease = claim(lifecycle.submit(command(3, null)).job().id(), "w1");
        UUID id = lease.job().id();
        clock.advance(Duration.ofSeconds(1));
        Job cancelled = lifecycle.cancel(id); // the worker died and never heard about it
        clock.set(lease.deadline().plus(LEASE_GRACE).plus(MS));
        Instant reclaimedAt = lifecycle.now();

        assertThat(reconciler.reconcileOnce()).isEqualTo(new Report(1, 0, 0, 0));

        assertThat(stored(id)).isEqualTo(cancelled);
        Delivery closed = repository.deliveries(id).get(0);
        assertThat(closed.ackState()).isEqualTo(AckState.EXPIRED);
        assertThat(closed.finishedAt()).isEqualTo(reclaimedAt);
        assertThat(closed.error()).isEqualTo("lease held by w1 expired at " + lease.deadline());
        assertThat(listener.count("leaseExpired")).isZero();
        assertThat(listener.count("attemptFinished")).isZero();
        assertThat(listener.count("retryScheduled")).isZero();
        assertThat(reconciler.reconcileOnce()).isEqualTo(NOTHING);
    }

    // ---------------------------------------------------------------- batches and reports

    @Test
    void batchSizeLimitsPendingRecoveriesPerPassOldestFirst() {
        build(repository, queue, 2);
        List<UUID> pending = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            pending.add(crashSubmit(Point.AFTER_PERSIST));
            clock.advance(MS);
        }
        clock.advance(PENDING_GRACE);

        assertThat(reconciler.reconcileOnce()).isEqualTo(new Report(0, 0, 2, 0));
        assertThat(drain(queue)).containsExactly(pending.get(0), pending.get(1));
        assertThat(reconciler.reconcileOnce()).isEqualTo(new Report(0, 0, 2, 0));
        assertThat(drain(queue)).containsExactly(pending.get(2), pending.get(3));
        assertThat(reconciler.reconcileOnce()).isEqualTo(new Report(0, 0, 1, 0));
        assertThat(drain(queue)).containsExactly(pending.get(4));
        assertThat(reconciler.reconcileOnce()).isEqualTo(NOTHING);
    }

    @Test
    void batchSizeLimitsLeaseReclaimsAndRedispatchesPerPass() {
        build(repository, queue, 2);
        for (int i = 0; i < 3; i++) {
            claim(lifecycle.submit(command(3, null)).job().id(), "w" + i);
            crashSubmit(Point.AFTER_ENQUEUE_BEFORE_DISPATCH);
            clock.advance(MS);
        }
        clock.advance(Duration.ofMinutes(1));

        assertThat(reconciler.reconcileOnce()).isEqualTo(new Report(2, 0, 0, 2));
        assertThat(reconciler.reconcileOnce()).isEqualTo(new Report(1, 0, 0, 1));
        assertThat(reconciler.reconcileOnce()).isEqualTo(NOTHING);
        assertThat(repository.countByState()).containsEntry(JobState.RETRY_WAIT, 3L)
                .containsEntry(JobState.QUEUED, 3L);
    }

    @Test
    void reportCountsEachKindOfRecovery() {
        Lease stale = claim(lifecycle.submit(command(3, null)).job().id(), "w-stale");
        Lease failing = claim(lifecycle.submit(command(3, null)).job().id(), "w-fail");
        lifecycle.fail(failing, new TaskFailure(FailureKind.RETRYABLE, "boom"));
        UUID pending = crashSubmit(Point.AFTER_PERSIST);
        UUID lost = crashSubmit(Point.AFTER_ENQUEUE_BEFORE_DISPATCH);
        drain(queue);
        clock.advance(Duration.ofMinutes(1));

        Report report = reconciler.reconcileOnce();

        assertThat(report).isEqualTo(new Report(1, 1, 1, 1));
        assertThat(report.total()).isEqualTo(4);
        assertThat(stored(stale.job().id()).state()).isEqualTo(JobState.RETRY_WAIT);
        assertThat(stored(failing.job().id()).state()).isEqualTo(JobState.QUEUED);
        assertThat(stored(pending).state()).isEqualTo(JobState.QUEUED);
        assertThat(stored(lost).dispatchedAt()).isEqualTo(lifecycle.now());
        assertThat(drain(queue)).containsExactlyInAnyOrder(failing.job().id(), pending, lost);
        assertThat(listener.labels()).filteredOn(l -> l.startsWith("recovered") || l.startsWith("leaseExpired"))
                .containsExactly("leaseExpired:1", "recovered:RETRY_RELEASED", "recovered:PENDING_ENQUEUED",
                        "recovered:QUEUED_REDISPATCHED");
        assertThat(reconciler.reconcileOnce()).isEqualTo(NOTHING);
    }

    @Test
    void settingsRejectNonPositiveBatchSize() {
        assertThatThrownBy(() -> settings(0)).isInstanceOf(IllegalArgumentException.class);
    }

    // ---------------------------------------------------------------- concurrency

    @Test
    void concurrentReconcilersRecoverEachJobExactlyOnce() throws Exception {
        int jobsPerKind = 20;
        int rounds = 10;
        executor = Executors.newFixedThreadPool(2);
        for (int round = 0; round < rounds; round++) {
            InMemoryDispatcher roundQueue = new InMemoryDispatcher();
            build(new InMemoryJobRepository(), roundQueue, 100);
            List<UUID> stale = new ArrayList<>();
            for (int i = 0; i < jobsPerKind; i++) {
                stale.add(claim(lifecycle.submit(command(3, null)).job().id(), "w" + i).job().id());
                crashSubmit(Point.AFTER_PERSIST);
            }
            drain(roundQueue);
            clock.advance(Duration.ofMinutes(1));
            Reconciler other = new Reconciler(repository, lifecycle, dispatcher, settings(100));
            CyclicBarrier barrier = new CyclicBarrier(2);
            Future<Report> first = executor.submit(() -> {
                barrier.await(5, SECONDS);
                return reconciler.reconcileOnce();
            });
            Future<Report> second = executor.submit(() -> {
                barrier.await(5, SECONDS);
                return other.reconcileOnce();
            });
            Report a = first.get(20, SECONDS);
            Report b = second.get(20, SECONDS);

            assertThat(a.leasesExpired() + b.leasesExpired()).as("round %d", round).isEqualTo(jobsPerKind);
            assertThat(a.pendingEnqueued() + b.pendingEnqueued()).as("round %d", round).isEqualTo(jobsPerKind);
            assertThat(a.retriesReleased() + b.retriesReleased()).isZero();
            assertThat(a.redispatched() + b.redispatched()).isZero();
            assertThat(repository.countByState()).containsOnlyKeys(JobState.RETRY_WAIT, JobState.QUEUED)
                    .containsEntry(JobState.RETRY_WAIT, (long) jobsPerKind)
                    .containsEntry(JobState.QUEUED, (long) jobsPerKind);
            for (UUID id : stale) {
                assertThat(stored(id).attempts()).isEqualTo(1);
                assertThat(stored(id).version()).isEqualTo(3);
                assertThat(repository.deliveries(id)).extracting(Delivery::ackState).containsExactly(AckState.EXPIRED);
            }
            List<UUID> dispatched = drain(roundQueue);
            assertThat(dispatched).hasSize(jobsPerKind);
            assertThat(new HashSet<>(dispatched)).hasSize(jobsPerKind);
            assertThat(listener.count("leaseExpired")).isEqualTo(jobsPerKind);
            assertThat(listener.count("recovered")).isEqualTo(jobsPerKind);
        }
    }

    // ---------------------------------------------------------------- helpers

    private UUID crashSubmit(Point point) {
        checkpoints.crashAt(point);
        try {
            assertThatThrownBy(() -> lifecycle.submit(command(3, null))).isInstanceOf(SimulatedCrash.class);
        } finally {
            checkpoints.clear();
        }
        return checkpoints.lastReached();
    }

    private Lease claim(UUID id, String worker) {
        return lifecycle.claim(id, worker).orElseThrow(() -> new AssertionError("claim of " + id + " failed"));
    }

    private Job stored(UUID id) {
        return repository.find(id).orElseThrow();
    }

    // ---------------------------------------------------------------- test doubles

    /** Records dispatched ids instead of queueing them; can fail once like an unavailable broker. */
    private static final class RecordingDispatcher implements JobDispatcher {
        private final boolean durable;
        private final List<UUID> dispatched = new CopyOnWriteArrayList<>();
        private final AtomicBoolean failNext = new AtomicBoolean();

        RecordingDispatcher(boolean durable) {
            this.durable = durable;
        }

        void failNext() {
            failNext.set(true);
        }

        List<UUID> dispatched() {
            return List.copyOf(dispatched);
        }

        @Override
        public void dispatch(String queue, UUID jobId) {
            if (failNext.compareAndSet(true, false)) throw new IllegalStateException("broker unavailable");
            dispatched.add(jobId);
        }

        @Override
        public Optional<UUID> poll(String queue, Duration timeout) {
            return Optional.empty();
        }

        @Override
        public boolean durable() {
            return durable;
        }
    }

    /** Delegates to a real repository and runs a hook right after the expired-lease scan returned its list. */
    private static final class RacingRepository implements JobRepository {
        private final JobRepository delegate;
        private volatile Runnable afterFindExpiredLeases = () -> { };

        RacingRepository(JobRepository delegate) {
            this.delegate = delegate;
        }

        void afterFindExpiredLeases(Runnable hook) {
            afterFindExpiredLeases = hook;
        }

        @Override
        public List<Delivery> findExpiredLeases(Instant before, int limit) {
            List<Delivery> expired = delegate.findExpiredLeases(before, limit);
            afterFindExpiredLeases.run();
            return expired;
        }

        @Override public InsertResult insert(Job job) { return delegate.insert(job); }
        @Override public Optional<Job> find(UUID id) { return delegate.find(id); }
        @Override public Optional<Job> findByIdempotencyKey(String key) { return delegate.findByIdempotencyKey(key); }
        @Override public boolean update(Job next, long expectedVersion) { return delegate.update(next, expectedVersion); }

        @Override
        public boolean claim(Job running, long expectedVersion, Delivery delivery) {
            return delegate.claim(running, expectedVersion, delivery);
        }

        @Override
        public boolean finishAttempt(Job next, long expectedVersion, Delivery closed, Instant leaseExpiredBefore) {
            return delegate.finishAttempt(next, expectedVersion, closed, leaseExpiredBefore);
        }

        @Override
        public boolean renewLease(UUID jobId, int attempt, String owner, long jobVersion, Instant newDeadline,
                                  Instant heartbeatAt, Integer progress) {
            return delegate.renewLease(jobId, attempt, owner, jobVersion, newDeadline, heartbeatAt, progress);
        }

        @Override
        public boolean closeDelivery(Delivery closed, Instant leaseExpiredBefore) {
            return delegate.closeDelivery(closed, leaseExpiredBefore);
        }

        @Override public void markDispatched(UUID id, Instant at) { delegate.markDispatched(id, at); }
        @Override public List<Delivery> deliveries(UUID jobId) { return delegate.deliveries(jobId); }
        @Override public List<Job> findDueRetries(Instant now, int limit) { return delegate.findDueRetries(now, limit); }

        @Override
        public List<Job> findStalePending(Instant before, int limit) {
            return delegate.findStalePending(before, limit);
        }

        @Override
        public List<Job> findQueuedDispatchedBefore(Instant before, int limit) {
            return delegate.findQueuedDispatchedBefore(before, limit);
        }

        @Override public QueueStats queueStats(String queue, Instant now) { return delegate.queueStats(queue, now); }
        @Override public Map<JobState, Long> countByState() { return delegate.countByState(); }
    }
}
