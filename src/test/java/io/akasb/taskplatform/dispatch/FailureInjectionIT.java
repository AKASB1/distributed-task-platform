package io.akasb.taskplatform.dispatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import io.akasb.taskplatform.domain.AckState;
import io.akasb.taskplatform.domain.Delivery;
import io.akasb.taskplatform.domain.Job;
import io.akasb.taskplatform.domain.JobState;
import io.akasb.taskplatform.domain.Lease;
import io.akasb.taskplatform.domain.RetryPolicy;
import io.akasb.taskplatform.observability.LifecycleListener;
import io.akasb.taskplatform.persistence.JdbcJobRepository;
import io.akasb.taskplatform.persistence.JobRepository;
import io.akasb.taskplatform.support.Await;
import io.akasb.taskplatform.support.EmbeddedPostgresSupport;
import io.akasb.taskplatform.support.MutableClock;
import io.akasb.taskplatform.worker.LocalWorkerProtocol;
import io.akasb.taskplatform.worker.TaskContext;
import io.akasb.taskplatform.worker.TaskHandler;
import io.akasb.taskplatform.worker.TaskHandlerRegistry;
import io.akasb.taskplatform.worker.WorkerPool;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Failure injection on real PostgreSQL: a control plane that crashes between the submission steps (recovered by a
 * fresh lifecycle, reconciler and dispatcher, as after a restart) and a worker pool killed in the middle of an
 * attempt (its lease expires, the job is retried elsewhere, and the zombie's work is never reported; a job cancelled
 * before the crash only has its orphaned lease closed). Time is a {@link MutableClock}, so no heartbeat or timeout
 * fires unless the test advances it.
 */
class FailureInjectionIT {
    private static final String QUEUE = "inject";
    private static final String TYPE = "work";
    private static final Duration LEASE = Duration.ofSeconds(30);
    private static final Reconciler.Settings RECONCILER = new Reconciler.Settings(Duration.ofSeconds(5),
            Duration.ofSeconds(10), Duration.ofSeconds(30), 100);
    private static final Duration WAIT = Duration.ofSeconds(20);

    private final ObjectMapper mapper = new ObjectMapper();
    private final List<WorkerPool> pools = new ArrayList<>();
    private final CountDownLatch releaseZombie = new CountDownLatch(1);
    private HikariDataSource dataSource;
    private JobRepository repository;
    private MutableClock clock;
    private RetryPolicy retryPolicy;
    private CountingListener listener;

    @BeforeEach
    void setUp() {
        dataSource = EmbeddedPostgresSupport.newMigratedDataSource(32);
        repository = new JdbcJobRepository(dataSource);
        clock = MutableClock.startingAt("2026-01-01T00:00:00Z");
        retryPolicy = new RetryPolicy(Duration.ofSeconds(1), 2.0, Duration.ofMinutes(1), 0.5, new Random(7));
        listener = new CountingListener();
    }

    @AfterEach
    void tearDown() {
        releaseZombie.countDown();
        try {
            pools.forEach(WorkerPool::close);
        } finally {
            dataSource.close();
        }
    }

    @Test
    void crashBetweenPersistAndEnqueueIsRecoveredAfterRestart() {
        AtomicReference<UUID> crashedJob = new AtomicReference<>();
        InMemoryDispatcher deadDispatcher = new InMemoryDispatcher();
        JobLifecycle crashing = lifecycle(deadDispatcher, crashAt(Checkpoints.Point.AFTER_PERSIST, crashedJob));

        assertThatThrownBy(() -> crashing.submit(command())).isInstanceOf(SimulatedCrash.class);
        UUID id = crashedJob.get();
        assertThat(id).isNotNull();
        Job pending = repository.find(id).orElseThrow();
        assertThat(pending.state()).isEqualTo(JobState.PENDING);
        assertThat(pending.version()).isZero();
        assertThat(repository.deliveries(id)).isEmpty();
        assertThat(deadDispatcher.depth(QUEUE)).isZero();

        // Restart: new dispatcher (the in-memory queue is gone), new lifecycle and reconciler, a running worker.
        InMemoryDispatcher dispatcher = new InMemoryDispatcher();
        JobLifecycle lifecycle = lifecycle(dispatcher, Checkpoints.none());
        Reconciler reconciler = new Reconciler(repository, lifecycle, dispatcher, RECONCILER);
        assertThat(reconciler.recoverOnStartup().pendingEnqueued()).as("still within pendingGrace").isZero();
        CountingHandler handler = new CountingHandler(Map.of());
        startPool("restarted", 1, dispatcher, lifecycle, handler);
        assertThat(repository.find(id).orElseThrow().state()).isEqualTo(JobState.PENDING);

        clock.advance(RECONCILER.pendingGrace().plusMillis(1));
        assertThat(reconciler.reconcileOnce().pendingEnqueued()).isEqualTo(1);

        Job done = awaitState(id, JobState.SUCCEEDED);
        assertThat(done.attempts()).isEqualTo(1);
        assertThat(done.result()).isEqualTo("{\"attempt\": 1}");
        assertSingleAckedDelivery(id, 1);
        assertThat(handler.completions(1)).isEqualTo(1);
        assertExactlyOnceAndQuiet(id, reconciler, done);
    }

    @Test
    void crashBetweenEnqueueAndDispatchIsRecoveredOnStartup() {
        AtomicReference<UUID> crashedJob = new AtomicReference<>();
        InMemoryDispatcher deadDispatcher = new InMemoryDispatcher();
        JobLifecycle crashing = lifecycle(deadDispatcher,
                crashAt(Checkpoints.Point.AFTER_ENQUEUE_BEFORE_DISPATCH, crashedJob));

        assertThatThrownBy(() -> crashing.submit(command())).isInstanceOf(SimulatedCrash.class);
        UUID id = crashedJob.get();
        Job queued = repository.find(id).orElseThrow();
        assertThat(queued.state()).isEqualTo(JobState.QUEUED);
        assertThat(queued.version()).isEqualTo(1);
        assertThat(deadDispatcher.depth(QUEUE)).as("never dispatched").isZero();

        InMemoryDispatcher dispatcher = new InMemoryDispatcher();
        JobLifecycle lifecycle = lifecycle(dispatcher, Checkpoints.none());
        Reconciler reconciler = new Reconciler(repository, lifecycle, dispatcher, RECONCILER);
        // Nothing to re-dispatch yet in a regular pass: the job was "dispatched" (enqueued) just now.
        assertThat(reconciler.reconcileOnce().total()).isZero();
        Reconciler.Report startup = reconciler.recoverOnStartup();
        assertThat(startup.redispatched()).isEqualTo(1);
        assertThat(dispatcher.depth(QUEUE)).isEqualTo(1);

        CountingHandler handler = new CountingHandler(Map.of());
        startPool("restarted", 1, dispatcher, lifecycle, handler);

        Job done = awaitState(id, JobState.SUCCEEDED);
        assertThat(done.attempts()).isEqualTo(1);
        assertSingleAckedDelivery(id, 1);
        assertThat(handler.completions(1)).isEqualTo(1);
        assertExactlyOnceAndQuiet(id, reconciler, done);
    }

    @Test
    void workerKilledMidJobIsReclaimedAndRetriedElsewhere() throws Exception {
        InMemoryDispatcher dispatcher = new InMemoryDispatcher();
        JobLifecycle lifecycle = lifecycle(dispatcher, Checkpoints.none());
        Reconciler reconciler = new Reconciler(repository, lifecycle, dispatcher, RECONCILER);
        CountDownLatch zombieStarted = new CountDownLatch(1);
        // Attempt 1 blocks (ignoring interrupts, like a stuck process) until the test releases it.
        CountingHandler handler = new CountingHandler(Map.of(1, new Gate(zombieStarted, releaseZombie)));
        WorkerPool poolA = startPool("pool-a", 1, dispatcher, lifecycle, handler);

        UUID id = lifecycle.submit(command()).job().id();
        assertThat(zombieStarted.await(WAIT.toSeconds(), TimeUnit.SECONDS)).as("attempt 1 started").isTrue();
        Job running = awaitState(id, JobState.RUNNING);
        assertThat(running.attempts()).isEqualTo(1);

        poolA.kill();
        Await.until("pool A stopped supervising", WAIT, () -> poolA.busy() == 0);
        Delivery leasedToA = repository.deliveries(id).get(0);
        assertThat(leasedToA.ackState()).isEqualTo(AckState.LEASED);

        clock.advance(LEASE.plus(RECONCILER.leaseGrace()).plusMillis(1));
        Reconciler.Report expiry = reconciler.reconcileOnce();
        assertThat(expiry.leasesExpired()).isEqualTo(1);
        assertThat(repository.find(id).orElseThrow().state()).isEqualTo(JobState.RETRY_WAIT);
        assertThat(repository.deliveries(id)).singleElement().extracting(Delivery::ackState)
                .isEqualTo(AckState.EXPIRED);

        clock.advance(retryPolicy.baseDelay(1));
        assertThat(reconciler.reconcileOnce().retriesReleased()).isEqualTo(1);
        assertThat(repository.find(id).orElseThrow().state()).isEqualTo(JobState.QUEUED);

        startPool("pool-b", 1, dispatcher, lifecycle, handler);
        Job done = awaitState(id, JobState.SUCCEEDED);
        assertThat(done.attempts()).isEqualTo(2);
        assertThat(done.result()).isEqualTo("{\"attempt\": 2}");
        assertThat(repository.deliveries(id))
                .extracting(Delivery::attempt, Delivery::leaseOwner, Delivery::ackState)
                .containsExactly(tuple(1, leasedToA.leaseOwner(), AckState.EXPIRED),
                        tuple(2, repository.deliveries(id).get(1).leaseOwner(), AckState.ACKED));
        assertThat(repository.deliveries(id).get(1).leaseOwner()).startsWith("pool-b@");
        assertThat(leasedToA.leaseOwner()).startsWith("pool-a@");

        // The zombie handler finishes its work now; nothing it did may reach the job.
        releaseZombie.countDown();
        Await.until("zombie handler returned", WAIT, () -> handler.completions(1) == 1);
        Lease zombieLease = listener.claimedLease(id, 1);
        assertThat(lifecycle.complete(zombieLease, "{\"by\":\"zombie\"}")).isEqualTo(Outcome.LEASE_LOST);
        assertThat(lifecycle.heartbeat(zombieLease, 100).status()).isEqualTo(HeartbeatResult.Status.LOST);

        Job after = repository.find(id).orElseThrow();
        assertThat(after.state()).isEqualTo(JobState.SUCCEEDED);
        assertThat(after.version()).isEqualTo(done.version());
        assertThat(after.result()).isEqualTo("{\"attempt\": 2}");
        List<Delivery> deliveries = repository.deliveries(id);
        assertThat(deliveries).filteredOn(d -> d.ackState() == AckState.ACKED)
                .singleElement().extracting(Delivery::attempt).isEqualTo(2);
        assertThat(handler.completions(2)).isEqualTo(1);
        assertThat(listener.acked(id)).as("job-level completions").isEqualTo(1);
    }

    @Test
    void cancelledJobWhoseWorkerCrashedIsClosedAndNeverRetried() throws Exception {
        InMemoryDispatcher dispatcher = new InMemoryDispatcher();
        JobLifecycle lifecycle = lifecycle(dispatcher, Checkpoints.none());
        Reconciler reconciler = new Reconciler(repository, lifecycle, dispatcher, RECONCILER);
        CountDownLatch started = new CountDownLatch(1);
        CountingHandler handler = new CountingHandler(Map.of(1, new Gate(started, releaseZombie)));
        WorkerPool pool = startPool("pool-c", 1, dispatcher, lifecycle, handler);

        UUID id = lifecycle.submit(command()).job().id();
        assertThat(started.await(WAIT.toSeconds(), TimeUnit.SECONDS)).isTrue();
        awaitState(id, JobState.RUNNING);
        Job cancelled = lifecycle.cancel(id);
        assertThat(cancelled.state()).isEqualTo(JobState.CANCELLED);
        // The clock is frozen, so the worker has not heartbeated since the cancel: it crashes without knowing.
        pool.kill();
        Await.until("pool C stopped supervising", WAIT, () -> pool.busy() == 0);
        assertThat(repository.deliveries(id)).singleElement().extracting(Delivery::ackState)
                .isEqualTo(AckState.LEASED);

        clock.advance(LEASE.plus(RECONCILER.leaseGrace()).plusMillis(1));
        assertThat(reconciler.reconcileOnce().leasesExpired()).as("orphaned lease closed").isEqualTo(1);
        assertThat(repository.deliveries(id)).singleElement().extracting(Delivery::ackState)
                .isEqualTo(AckState.EXPIRED);

        clock.advance(retryPolicy.maxDelay());
        assertThat(reconciler.reconcileOnce().total()).isZero();
        Job after = repository.find(id).orElseThrow();
        assertThat(after.state()).isEqualTo(JobState.CANCELLED);
        assertThat(after.version()).isEqualTo(cancelled.version());
        assertThat(after.attempts()).isEqualTo(1);
        assertThat(repository.deliveries(id)).hasSize(1);
        assertThat(listener.acked(id)).isZero();
    }

    // ------------------------------------------------------------------------------------------ helpers

    private JobLifecycle lifecycle(JobDispatcher dispatcher, Checkpoints checkpoints) {
        return new JobLifecycle(repository, dispatcher, retryPolicy, clock, LEASE, listener, checkpoints);
    }

    private static Checkpoints crashAt(Checkpoints.Point crashPoint, AtomicReference<UUID> job) {
        return (point, jobId) -> {
            if (point == crashPoint) {
                job.set(jobId);
                throw new SimulatedCrash(point);
            }
        };
    }

    private static SubmitCommand command() {
        return new SubmitCommand(QUEUE, TYPE, "{}", 3, Duration.ofMinutes(10), null);
    }

    private WorkerPool startPool(String name, int concurrency, JobDispatcher dispatcher, JobLifecycle lifecycle,
                                 TaskHandler handler) {
        WorkerPool pool = new WorkerPool(new WorkerPool.Settings(name, QUEUE, concurrency, Duration.ofMillis(20),
                Duration.ofMillis(50), Duration.ofSeconds(2)), new LocalWorkerProtocol(dispatcher, lifecycle),
                new TaskHandlerRegistry(List.of(handler)), clock, mapper);
        pools.add(pool);
        pool.start();
        return pool;
    }

    private Job awaitState(UUID id, JobState state) {
        return Await.value("job " + id + " in " + state, WAIT, () -> repository.find(id).orElseThrow(),
                j -> j.state() == state);
    }

    private void assertSingleAckedDelivery(UUID id, int attempt) {
        assertThat(repository.deliveries(id)).singleElement().satisfies(d -> {
            assertThat(d.attempt()).isEqualTo(attempt);
            assertThat(d.ackState()).isEqualTo(AckState.ACKED);
        });
    }

    /** Succeeded once, and later reconciler passes find nothing more to do for it. */
    private void assertExactlyOnceAndQuiet(UUID id, Reconciler reconciler, Job done) {
        assertThat(listener.acked(id)).isEqualTo(1);
        clock.advance(RECONCILER.redispatchAfter().plus(RECONCILER.leaseGrace()).plus(LEASE));
        assertThat(reconciler.reconcileOnce().total()).isZero();
        assertThat(reconciler.recoverOnStartup().total()).isZero();
        assertThat(repository.find(id).orElseThrow().version()).isEqualTo(done.version());
        assertThat(repository.deliveries(id)).hasSize(1);
    }

    /** Thrown by an injected checkpoint: the "process" dies at that point. */
    private static final class SimulatedCrash extends RuntimeException {
        SimulatedCrash(Checkpoints.Point point) {
            super("simulated crash at " + point);
        }
    }

    /** Blocks an attempt until released, ignoring interrupts (a stuck or partitioned worker). */
    private record Gate(CountDownLatch started, CountDownLatch release) {
        void pass() {
            started.countDown();
            boolean interrupted = false;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            try {
                while (true) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) return;
                    try {
                        if (release.await(remaining, TimeUnit.NANOSECONDS)) return;
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
            } finally {
                if (interrupted) Thread.currentThread().interrupt();
            }
        }
    }

    /** Returns {"attempt": n}; counts completed handler runs per attempt and optionally gates an attempt. */
    private static final class CountingHandler implements TaskHandler {
        private final Map<Integer, Gate> gates;
        private final Map<Integer, AtomicInteger> completions = new ConcurrentHashMap<>();

        CountingHandler(Map<Integer, Gate> gates) {
            this.gates = gates;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public Object handle(TaskContext context) {
            Gate gate = gates.get(context.attempt());
            if (gate != null) gate.pass();
            completions.computeIfAbsent(context.attempt(), a -> new AtomicInteger()).incrementAndGet();
            return Map.of("attempt", context.attempt());
        }

        int completions(int attempt) {
            AtomicInteger c = completions.get(attempt);
            return c == null ? 0 : c.get();
        }
    }

    /** Records claims (to rebuild a zombie's lease) and counts job-level completions. */
    private static final class CountingListener implements LifecycleListener {
        private final List<Lease> claims = new CopyOnWriteArrayList<>();
        private final Map<UUID, AtomicInteger> acked = new ConcurrentHashMap<>();

        @Override
        public void onClaimed(Job running, Delivery delivery) {
            claims.add(new Lease(running, delivery));
        }

        @Override
        public void onAttemptFinished(Job next, Delivery closed) {
            if (closed.ackState() == AckState.ACKED) {
                acked.computeIfAbsent(next.id(), k -> new AtomicInteger()).incrementAndGet();
            }
        }

        int acked(UUID id) {
            AtomicInteger c = acked.get(id);
            return c == null ? 0 : c.get();
        }

        Lease claimedLease(UUID id, int attempt) {
            return claims.stream().filter(l -> l.job().id().equals(id) && l.attempt() == attempt).findFirst()
                    .orElseThrow();
        }
    }
}
