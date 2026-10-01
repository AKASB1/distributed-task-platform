package io.akasb.taskplatform.worker;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.akasb.taskplatform.dispatch.Checkpoints;
import io.akasb.taskplatform.dispatch.HeartbeatResult;
import io.akasb.taskplatform.dispatch.InMemoryDispatcher;
import io.akasb.taskplatform.dispatch.JobLifecycle;
import io.akasb.taskplatform.dispatch.Outcome;
import io.akasb.taskplatform.dispatch.Reconciler;
import io.akasb.taskplatform.dispatch.SubmitCommand;
import io.akasb.taskplatform.domain.AckState;
import io.akasb.taskplatform.domain.Delivery;
import io.akasb.taskplatform.domain.Job;
import io.akasb.taskplatform.domain.JobState;
import io.akasb.taskplatform.domain.Lease;
import io.akasb.taskplatform.domain.RetryPolicy;
import io.akasb.taskplatform.domain.TaskFailure;
import io.akasb.taskplatform.observability.LifecycleListener;
import io.akasb.taskplatform.persistence.InMemoryJobRepository;
import io.akasb.taskplatform.support.Await;
import io.akasb.taskplatform.support.MutableClock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * {@link WorkerPool} on the in-memory stack (repository, dispatcher, real {@link JobLifecycle} and
 * {@link LocalWorkerProtocol}). Deadlines, heartbeats and leases follow a {@link MutableClock} that only the test
 * moves; the supervision loop wakes every {@value #HEARTBEAT_MS} ms of real time, so each clock step is picked up
 * quickly and asynchronous outcomes are awaited by polling.
 */
class WorkerPoolTest {
    private static final long HEARTBEAT_MS = 20;
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
    private static final String QUEUE = "work";
    private static final Duration LEASE = Duration.ofSeconds(5);
    private static final Duration LEASE_GRACE = Duration.ofSeconds(1);
    private static final Duration HEARTBEAT = Duration.ofMillis(HEARTBEAT_MS);
    private static final Duration POLL = Duration.ofMillis(50);
    private static final Duration GRACE = Duration.ofSeconds(5);
    private static final Duration LONG_TIMEOUT = Duration.ofHours(1);
    private static final Duration AWAIT = Duration.ofSeconds(5);
    private static final Duration NUDGE_AFTER = Duration.ofMillis(100);

    private final ObjectMapper mapper = new ObjectMapper();
    private final List<WorkerPool> pools = new ArrayList<>();
    private final List<Releasable> releasables = new ArrayList<>();
    private MutableClock clock;
    private InMemoryJobRepository repository;
    private JobLifecycle lifecycle;
    private RecordingProtocol protocol;
    private RecordingListener listener;
    private Reconciler reconciler;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(START);
        repository = new InMemoryJobRepository();
        InMemoryDispatcher dispatcher = new InMemoryDispatcher();
        listener = new RecordingListener();
        // After the first failed attempt the backoff is in [500 ms, 1000 ms].
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(1), 2.0, Duration.ofMinutes(1), 0.5, new Random(42));
        lifecycle = new JobLifecycle(repository, dispatcher, retry, clock, LEASE, listener, Checkpoints.none());
        protocol = new RecordingProtocol(new LocalWorkerProtocol(dispatcher, lifecycle));
        reconciler = new Reconciler(repository, lifecycle, dispatcher,
                new Reconciler.Settings(LEASE_GRACE, Duration.ofMinutes(10), Duration.ofMinutes(10), 100));
    }

    @AfterEach
    void tearDown() {
        protocol.resumeAfterLease();
        releasables.forEach(Releasable::releaseAll);
        pools.forEach(WorkerPool::stop); // no-op for pools that were already stopped or killed
    }

    // ------------------------------------------------------------------------------------------ outcomes

    @Test
    void successfulAttemptStoresTheResultAndAcknowledgesTheDelivery() throws Exception {
        TaskHandler echo = new FnHandler("echo", context -> {
            context.reportProgress(30);
            ObjectNode result = mapper.createObjectNode();
            result.put("echo", context.payload().path("value").asText());
            result.put("attempt", context.attempt());
            result.put("queue", context.queue());
            result.put("jobId", context.jobId().toString());
            return result;
        });
        WorkerPool pool = startPool("main", QUEUE, 1, GRACE, echo);
        UUID id = submit(QUEUE, "echo", "{\"value\":\"hi\"}", 3, LONG_TIMEOUT);

        Job done = awaitState(id, JobState.SUCCEEDED);
        assertThat(mapper.readTree(done.result())).isEqualTo(mapper.readTree(
                "{\"echo\":\"hi\",\"attempt\":1,\"queue\":\"work\",\"jobId\":\"" + id + "\"}"));
        assertThat(done.attempts()).isEqualTo(1);
        assertThat(done.version()).as("submit, claim, complete").isEqualTo(3);
        assertThat(done.lastError()).isNull();
        assertThat(done.finishedAt()).isEqualTo(START);

        assertThat(repository.deliveries(id)).hasSize(1);
        Delivery delivery = delivery(id);
        assertThat(delivery.attempt()).isEqualTo(1);
        assertThat(delivery.ackState()).isEqualTo(AckState.ACKED);
        assertThat(delivery.progress()).isEqualTo(100);
        assertThat(delivery.leaseOwner()).startsWith("main@");
        assertThat(delivery.finishedAt()).isEqualTo(START);
        assertThat(delivery.error()).isNull();
        assertThat(protocol.reports(id)).containsExactly(new Call(id, "complete", "ACCEPTED"));
        Await.until("slot idle again", AWAIT, () -> pool.busy() == 0);
    }

    @Test
    void nonRetryableFailureFailsTheJobWithoutRetrying() {
        startPool("main", QUEUE, 1, GRACE, new FnHandler("reject", context -> {
            throw new NonRetryableTaskException("bad input");
        }));
        UUID id = submit(QUEUE, "reject", "{}", 3, LONG_TIMEOUT);

        Job failed = awaitState(id, JobState.FAILED);
        assertThat(failed.attempts()).isEqualTo(1);
        assertThat(failed.lastError()).isEqualTo("NON_RETRYABLE: bad input");
        assertThat(failed.nextAttemptAt()).isNull();
        assertThat(failed.finishedAt()).isEqualTo(START);
        assertThat(delivery(id).ackState()).isEqualTo(AckState.NACKED);
        assertThat(delivery(id).error()).isEqualTo("NON_RETRYABLE: bad input");
        assertThat(protocol.reports(id)).containsExactly(new Call(id, "fail", "NON_RETRYABLE ACCEPTED"));
    }

    @Test
    void otherExceptionsAreRetriedWithBackoffAndTheNextClaimIsANewAttempt() throws Exception {
        startPool("main", QUEUE, 1, GRACE, new FnHandler("flaky", context -> {
            if (context.attempt() == 1) throw new IllegalStateException("boom");
            return Map.of("attempt", context.attempt());
        }));
        UUID id = submit(QUEUE, "flaky", "{}", 3, LONG_TIMEOUT);

        Job waiting = awaitState(id, JobState.RETRY_WAIT);
        assertThat(waiting.lastError()).isEqualTo("RETRYABLE: IllegalStateException: boom");
        assertThat(waiting.nextAttemptAt()).isBetween(START.plusMillis(500), START.plusMillis(1000));
        assertThat(delivery(id).ackState()).isEqualTo(AckState.NACKED);
        assertThat(delivery(id).error()).isEqualTo("RETRYABLE: IllegalStateException: boom");

        clock.advance(Duration.ofSeconds(1));
        assertThat(reconciler.reconcileOnce().retriesReleased()).isEqualTo(1);
        Job done = awaitState(id, JobState.SUCCEEDED);
        assertThat(done.attempts()).isEqualTo(2);
        assertThat(done.lastError()).isNull();
        assertThat(mapper.readTree(done.result())).isEqualTo(mapper.readTree("{\"attempt\":2}"));
        assertThat(repository.deliveries(id)).extracting(Delivery::attempt).containsExactly(1, 2);
        assertThat(repository.deliveries(id)).extracting(Delivery::ackState)
                .containsExactly(AckState.NACKED, AckState.ACKED);
    }

    @Test
    void retryableFailureOnTheLastAttemptIsDeadLettered() {
        startPool("main", QUEUE, 1, GRACE, new FnHandler("broken", context -> {
            throw new RetryableTaskException("still down");
        }));
        UUID id = submit(QUEUE, "broken", "{}", 1, LONG_TIMEOUT);

        Job dead = awaitState(id, JobState.DEAD_LETTER);
        assertThat(dead.lastError())
                .isEqualTo("attempts exhausted (1/1); last error: RETRYABLE: RetryableTaskException: still down");
        assertThat(delivery(id).ackState()).isEqualTo(AckState.NACKED);
    }

    @Test
    void jobWithoutAHandlerFailsPermanently() {
        GateHandler known = register(new GateHandler("known", false));
        startPool("main", QUEUE, 1, GRACE, known);
        UUID id = submit(QUEUE, "mystery", "{}", 3, LONG_TIMEOUT);

        Job failed = awaitState(id, JobState.FAILED);
        assertThat(failed.attempts()).isEqualTo(1);
        assertThat(failed.lastError()).isEqualTo("NON_RETRYABLE: no handler for type mystery");
        assertThat(delivery(id).ackState()).isEqualTo(AckState.NACKED);
        assertThat(known.started).isEmpty();
    }

    @Test
    void unparseablePayloadFailsPermanentlyWithoutCallingTheHandler() {
        GateHandler handler = register(new GateHandler("block", false));
        startPool("main", QUEUE, 1, GRACE, handler);
        UUID id = submit(QUEUE, "block", "{not json", 3, LONG_TIMEOUT);

        Job failed = awaitState(id, JobState.FAILED);
        assertThat(failed.lastError()).isEqualTo("NON_RETRYABLE: invalid payload");
        assertThat(delivery(id).ackState()).isEqualTo(AckState.NACKED);
        assertThat(handler.started).isEmpty();
    }

    // ------------------------------------------------------------------------------------------ supervision

    @Test
    void heartbeatsRenewTheLeaseAndPersistProgressWhileTheHandlerOutlivesTheLease() throws Exception {
        ProgressHandler handler = register(new ProgressHandler());
        startPool("main", QUEUE, 1, GRACE, handler);
        UUID id = submit(QUEUE, "progress", "{}", 3, LONG_TIMEOUT);
        assertThat(handler.started.await(AWAIT.toMillis(), MILLISECONDS)).isTrue();
        Delivery initial = delivery(id);
        long claimedVersion = job(id).version();
        assertThat(initial.leaseDeadline()).isEqualTo(START.plus(LEASE));

        int[] reported = {10, 25, 60, 150};
        int[] persisted = {10, 25, 60, 100}; // TaskContext clamps to 0..100
        Delivery previous = initial;
        for (int i = 0; i < reported.length; i++) {
            handler.report(reported[i]);
            Instant before = previous.heartbeatAt();
            Integer expected = persisted[i];
            advanceUntil("heartbeat carrying progress " + expected, Duration.ofSeconds(2), () -> {
                Delivery d = delivery(id);
                return d.heartbeatAt().isAfter(before) && Objects.equals(d.progress(), expected);
            });
            Delivery renewed = delivery(id);
            assertThat(renewed.ackState()).isEqualTo(AckState.LEASED);
            assertThat(renewed.leaseDeadline()).isEqualTo(renewed.heartbeatAt().plus(LEASE))
                    .isAfter(previous.leaseDeadline());
            assertThat(job(id).version()).as("lease renewals only touch the delivery row").isEqualTo(claimedVersion);
            previous = renewed;
        }

        assertThat(lifecycle.now()).as("without renewals the first lease would be reclaimable by now")
                .isAfter(initial.leaseDeadline().plus(LEASE_GRACE));
        assertThat(reconciler.reconcileOnce().leasesExpired()).isZero();
        assertThat(job(id).state()).isEqualTo(JobState.RUNNING);
        assertThat(delivery(id).ackState()).isEqualTo(AckState.LEASED);
        assertThat(protocol.heartbeats(id)).isNotEmpty().containsOnly("RENEWED");

        handler.releaseAll();
        Job done = awaitState(id, JobState.SUCCEEDED);
        assertThat(done.attempts()).isEqualTo(1);
        assertThat(delivery(id).ackState()).isEqualTo(AckState.ACKED);
        assertThat(delivery(id).progress()).isEqualTo(100);
    }

    @Test
    void executionTimeoutInterruptsTheHandlerAndRetriesUntilTheLastAttemptIsDeadLettered() throws Exception {
        GateHandler handler = register(new GateHandler("block", false));
        startPool("main", QUEUE, 1, GRACE, handler);
        Duration timeout = Duration.ofSeconds(2);
        UUID id = submit(QUEUE, "block", "{}", 2, timeout);

        TaskContext first = handler.awaitStart();
        clock.advance(timeout); // the deadline is startedAt + timeout on the injected clock
        Job waiting = awaitState(id, JobState.RETRY_WAIT);
        Await.until("first attempt interrupted", AWAIT, () -> handler.interruptions.get() == 1);
        assertThat(first.isCancelled()).isTrue();
        Instant failedAt = START.plus(timeout);
        assertThat(waiting.attempts()).isEqualTo(1);
        assertThat(waiting.lastError()).isEqualTo("TIMEOUT: exceeded timeout of 2000 ms");
        assertThat(waiting.nextAttemptAt()).isBetween(failedAt.plusMillis(500), failedAt.plusMillis(1000));
        assertThat(delivery(id).ackState()).isEqualTo(AckState.NACKED);
        assertThat(delivery(id).error()).isEqualTo("TIMEOUT: exceeded timeout of 2000 ms");

        clock.advance(Duration.ofSeconds(1)); // backoff elapsed
        assertThat(reconciler.reconcileOnce().retriesReleased()).isEqualTo(1);
        TaskContext second = handler.awaitStart();
        assertThat(second.attempt()).isEqualTo(2);
        clock.advance(timeout);

        Job dead = awaitState(id, JobState.DEAD_LETTER);
        Await.until("second attempt interrupted", AWAIT, () -> handler.interruptions.get() == 2);
        assertThat(dead.attempts()).isEqualTo(2);
        assertThat(dead.lastError())
                .isEqualTo("attempts exhausted (2/2); last error: TIMEOUT: exceeded timeout of 2000 ms");
        assertThat(repository.deliveries(id)).extracting(Delivery::ackState)
                .containsExactly(AckState.NACKED, AckState.NACKED);
        assertThat(protocol.reports(id)).extracting(Call::result)
                .containsExactly("TIMEOUT ACCEPTED", "TIMEOUT ACCEPTED");
    }

    @Test
    void cancellingARunningJobStopsTheHandlerAtTheNextHeartbeatAndDiscardsItsResult() throws Exception {
        GateHandler handler = register(new GateHandler("block", true)); // returns a result even when interrupted
        WorkerPool pool = startPool("main", QUEUE, 1, GRACE, handler);
        UUID id = submit(QUEUE, "block", "{}", 3, LONG_TIMEOUT);
        TaskContext context = handler.awaitStart();

        Job cancelled = lifecycle.cancel(id);
        assertThat(cancelled.state()).isEqualTo(JobState.CANCELLED);
        assertThat(delivery(id).ackState()).as("the worker learns about it at its next heartbeat")
                .isEqualTo(AckState.LEASED);
        assertThat(handler.interruptions).hasValue(0);

        advanceUntil("handler interrupted after the next heartbeat", Duration.ofSeconds(1),
                () -> handler.interruptions.get() == 1);
        Delivery closed = Await.value("delivery closed", AWAIT, () -> delivery(id),
                d -> d.ackState() != AckState.LEASED);
        assertThat(closed.ackState()).isEqualTo(AckState.CANCELLED);
        assertThat(closed.error()).isEqualTo("job cancelled");
        assertThat(context.isCancelled()).isTrue();
        Await.until("handler returned its late result", AWAIT, () -> handler.exited.get() == 1);
        Await.until("slot idle again", AWAIT, () -> pool.busy() == 0);

        Job after = job(id);
        assertThat(after.state()).isEqualTo(JobState.CANCELLED);
        assertThat(after.version()).isEqualTo(cancelled.version());
        assertThat(after.result()).isNull();
        assertThat(listener.states(id)).doesNotContain(JobState.SUCCEEDED);
        assertThat(protocol.heartbeats(id)).containsExactly("CANCELLED");
        assertThat(protocol.reports(id)).extracting(Call::kind).containsExactly("acknowledgeCancel");
    }

    @Test
    void cancelledJobWhoseHandlerFinishesBeforeTheNextHeartbeatNeverSucceeds() throws Exception {
        GateHandler handler = register(new GateHandler("block", false));
        WorkerPool pool = startPool("main", QUEUE, 1, GRACE, handler);
        UUID id = submit(QUEUE, "block", "{}", 3, LONG_TIMEOUT);
        handler.awaitStart();

        Job cancelled = lifecycle.cancel(id);
        handler.release(1); // the clock is frozen, so the handler returns before any heartbeat
        Call report = Await.value("the completion report", AWAIT, () -> protocol.reports(id), r -> !r.isEmpty())
                .get(0);
        assertThat(report).isEqualTo(new Call(id, "complete", "CANCELLED"));
        Await.until("slot idle again", AWAIT, () -> pool.busy() == 0);

        Job after = job(id);
        assertThat(after.state()).isEqualTo(JobState.CANCELLED);
        assertThat(after.version()).isEqualTo(cancelled.version());
        assertThat(after.result()).isNull();
        assertThat(listener.states(id)).doesNotContain(JobState.SUCCEEDED);
    }

    /**
     * The worker learns about the cancellation from the rejected completion ({@link Outcome#CANCELLED}) instead of
     * a heartbeat; the attempt should still be acknowledged as cancelled rather than left LEASED until the
     * reconciler closes it as EXPIRED.
     */
    @Test
    void cancelledJobWhoseHandlerFinishesBeforeTheNextHeartbeatClosesItsDeliveryAsCancelled() throws Exception {
        GateHandler handler = register(new GateHandler("block", false));
        WorkerPool pool = startPool("main", QUEUE, 1, GRACE, handler);
        UUID id = submit(QUEUE, "block", "{}", 3, LONG_TIMEOUT);
        handler.awaitStart();

        lifecycle.cancel(id);
        handler.release(1);
        Await.until("the completion report", AWAIT, () -> !protocol.reports(id).isEmpty());
        Await.until("slot idle again", AWAIT, () -> pool.busy() == 0);

        assertThat(delivery(id).ackState())
                .as("delivery of an attempt that was told its job is CANCELLED (reports: %s)", protocol.reports(id))
                .isEqualTo(AckState.CANCELLED);
    }

    @Test
    void lostLeaseStopsTheHandlerAndNothingIsReported() throws Exception {
        GateHandler handler = register(new GateHandler("block", false));
        WorkerPool pool = startPool("main", QUEUE, 1, GRACE, handler);
        UUID id = submit(QUEUE, "block", "{}", 3, LONG_TIMEOUT);
        TaskContext first = handler.awaitStart();

        protocol.heartbeatsDown = true; // e.g. a partition between this worker and the control plane
        clock.advance(LEASE.plus(LEASE_GRACE).plusSeconds(1));
        assertThat(reconciler.reconcileOnce().leasesExpired()).isEqualTo(1);
        Job reclaimed = job(id);
        assertThat(reclaimed.state()).isEqualTo(JobState.RETRY_WAIT);
        assertThat(reclaimed.lastError()).startsWith("LEASE_EXPIRED: lease held by main@");
        Delivery expired = delivery(id);
        assertThat(expired.ackState()).isEqualTo(AckState.EXPIRED);

        protocol.heartbeatsDown = false;
        advanceUntil("handler stopped after the lease was lost", Duration.ofSeconds(1),
                () -> handler.interruptions.get() == 1);
        Await.until("slot idle again", AWAIT, () -> pool.busy() == 0);
        assertThat(first.isCancelled()).isTrue();
        assertThat(protocol.heartbeats(id)).doesNotContain("RENEWED").last().isEqualTo("LOST");
        assertThat(protocol.reports(id)).as("an attempt that lost its lease reports nothing").isEmpty();
        assertThat(job(id)).isEqualTo(reclaimed);
        assertThat(repository.deliveries(id)).containsExactly(expired);

        // The reclaimed job runs again as a new attempt.
        clock.advance(Duration.ofSeconds(1));
        assertThat(reconciler.reconcileOnce().retriesReleased()).isEqualTo(1);
        assertThat(handler.awaitStart().attempt()).isEqualTo(2);
        handler.release(1);
        Job done = awaitState(id, JobState.SUCCEEDED);
        assertThat(done.attempts()).isEqualTo(2);
        assertThat(repository.deliveries(id)).extracting(Delivery::ackState)
                .containsExactly(AckState.EXPIRED, AckState.ACKED);
    }

    @Test
    void completionReportedAfterTheLeaseWasReclaimedIsRejected() throws Exception {
        GateHandler handler = register(new GateHandler("block", false));
        WorkerPool pool = startPool("main", QUEUE, 1, GRACE, handler);
        UUID id = submit(QUEUE, "block", "{}", 3, LONG_TIMEOUT);
        handler.awaitStart();

        protocol.heartbeatsDown = true;
        clock.advance(LEASE.plus(LEASE_GRACE).plusSeconds(1));
        assertThat(reconciler.reconcileOnce().leasesExpired()).isEqualTo(1);
        Job reclaimed = job(id);

        handler.release(1); // the stalled handler finishes after all
        Call report = Await.value("the completion report", AWAIT, () -> protocol.reports(id), r -> !r.isEmpty())
                .get(0);
        assertThat(report).isEqualTo(new Call(id, "complete", "LEASE_LOST"));
        Await.until("slot idle again", AWAIT, () -> pool.busy() == 0);
        assertThat(job(id)).isEqualTo(reclaimed);
        assertThat(job(id).result()).isNull();
        assertThat(repository.deliveries(id)).extracting(Delivery::ackState).containsExactly(AckState.EXPIRED);
        assertThat(listener.states(id)).doesNotContain(JobState.SUCCEEDED);
    }

    // ------------------------------------------------------------------------------------------ stop and kill

    @Test
    void stopOfAnIdlePoolDoesNotWaitForTheGracePeriod() {
        WorkerPool pool = startPool("idle", QUEUE, 3, Duration.ofSeconds(30), new FnHandler("noop", c -> null));

        long started = System.nanoTime();
        pool.stop();
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(5));
        assertThat(pool.busy()).isZero();
        pool.stop(); // idempotent
    }

    @Test
    void stopLetsARunningAttemptFinishWithinTheGracePeriodAndTakesNoNewWork() throws Exception {
        GateHandler handler = register(new GateHandler("block", false));
        WorkerPool pool = startPool("main", QUEUE, 1, Duration.ofSeconds(30), handler);
        UUID first = submit(QUEUE, "block", "{}", 3, LONG_TIMEOUT);
        UUID second = submit(QUEUE, "block", "{}", 3, LONG_TIMEOUT);
        assertThat(handler.awaitStart().jobId()).isEqualTo(first);

        Thread stopper = Thread.ofPlatform().name("stopper").start(pool::stop);
        awaitInsideStop(stopper);
        handler.release(1);
        assertThat(stopper.join(AWAIT)).as("stop() returned").isTrue();

        assertThat(job(first).state()).isEqualTo(JobState.SUCCEEDED);
        assertThat(delivery(first).ackState()).isEqualTo(AckState.ACKED);
        assertThat(handler.interruptions).hasValue(0);
        assertThat(job(second).state()).as("a stopping pool takes no new work").isEqualTo(JobState.QUEUED);
        assertThat(repository.deliveries(second)).isEmpty();
    }

    @Test
    void stopInterruptsAnAttemptThatOutlastsTheGracePeriodAndReportsWorkerShutdown() throws Exception {
        GateHandler handler = register(new GateHandler("block", false));
        Duration grace = Duration.ofMillis(200);
        WorkerPool pool = startPool("main", QUEUE, 1, grace, handler);
        UUID id = submit(QUEUE, "block", "{}", 3, LONG_TIMEOUT);
        TaskContext context = handler.awaitStart();

        long started = System.nanoTime();
        pool.stop();
        assertThat(Duration.ofNanos(System.nanoTime() - started)).as("the grace period was granted")
                .isGreaterThanOrEqualTo(grace);

        Job waiting = job(id);
        assertThat(waiting.state()).isEqualTo(JobState.RETRY_WAIT);
        assertThat(waiting.lastError()).isEqualTo("WORKER_SHUTDOWN: worker pool stopped during the attempt");
        assertThat(delivery(id).ackState()).isEqualTo(AckState.NACKED);
        assertThat(delivery(id).error()).isEqualTo("WORKER_SHUTDOWN: worker pool stopped during the attempt");
        assertThat(protocol.reports(id)).containsExactly(new Call(id, "fail", "WORKER_SHUTDOWN ACCEPTED"));
        Await.until("handler interrupted", AWAIT, () -> handler.interruptions.get() == 1);
        assertThat(context.isCancelled()).isTrue();
        assertThat(pool.busy()).isZero();
    }

    /**
     * stop() treats a slot as idle while it is inside {@code acquireNext}. If that slot's claim (a database round
     * trip that does not react to interrupts) succeeds anyway, the freshly leased attempt is a running attempt and
     * must get the grace period like any other.
     */
    @Test
    void attemptLeasedWhileStopInterruptsIdleSlotsStillGetsTheGracePeriod() throws Exception {
        GateHandler handler = register(new GateHandler("block", false));
        protocol.pauseNextLease();
        WorkerPool pool = startPool("main", QUEUE, 1, Duration.ofSeconds(30), handler);
        UUID id = submit(QUEUE, "block", "{}", 3, LONG_TIMEOUT);
        protocol.awaitPausedWithLease();
        assertThat(job(id).state()).isEqualTo(JobState.RUNNING);

        Thread stopper = Thread.ofPlatform().name("stopper").start(pool::stop);
        awaitInsideStop(stopper); // the idle-slot interrupt has been delivered
        Instant leasedAt = delivery(id).heartbeatAt();
        protocol.resumeAfterLease();
        advanceUntil("the leased attempt is supervised (heartbeat) or given up", Duration.ofSeconds(1),
                () -> delivery(id).heartbeatAt().isAfter(leasedAt) || job(id).state() != JobState.RUNNING);
        Job during = job(id);
        assertThat(during.state())
                .as("attempt leased while stop() was starting (last error: %s)", during.lastError())
                .isEqualTo(JobState.RUNNING);

        handler.release(1);
        assertThat(stopper.join(AWAIT)).as("stop() returned").isTrue();
        assertThat(job(id).state()).isEqualTo(JobState.SUCCEEDED);
    }

    @Test
    void killStopsWithoutReportingAndLeavesTheLeaseToExpire() throws Exception {
        GateHandler handler = register(new GateHandler("block", true)); // would even return a result
        WorkerPool pool = startPool("main", QUEUE, 1, GRACE, handler);
        UUID id = submit(QUEUE, "block", "{}", 3, LONG_TIMEOUT);
        handler.awaitStart();
        Delivery leased = delivery(id);
        Job running = job(id);

        pool.kill();
        Await.until("handler interrupted and returned", AWAIT,
                () -> handler.interruptions.get() == 1 && handler.exited.get() == 1);
        Await.until("slot abandoned the attempt", AWAIT, () -> pool.busy() == 0);
        assertThat(protocol.reports(id)).as("a crashed worker reports nothing").isEmpty();
        assertThat(delivery(id)).isEqualTo(leased);
        assertThat(delivery(id).ackState()).isEqualTo(AckState.LEASED);
        assertThat(job(id)).isEqualTo(running);
        assertThatThrownBy(pool::start).isInstanceOf(IllegalStateException.class);

        // Nobody renews the lease any more; it expires and the reconciler recovers the job.
        clock.advance(LEASE.plus(LEASE_GRACE).plusSeconds(1));
        assertThat(reconciler.reconcileOnce().leasesExpired()).isEqualTo(1);
        assertThat(job(id).state()).isEqualTo(JobState.RETRY_WAIT);
        assertThat(job(id).lastError()).startsWith("LEASE_EXPIRED");
        assertThat(delivery(id).ackState()).isEqualTo(AckState.EXPIRED);
        assertThat(protocol.heartbeats(id)).isEmpty();
    }

    // ------------------------------------------------------------------------------------------ capacity

    @Test
    void busyUtilizationAndBusyTimeTrackRunningAttempts() throws Exception {
        GateHandler handler = register(new GateHandler("block", false));
        WorkerPool pool = startPool("metrics", QUEUE, 4, GRACE, handler);
        assertThat(pool.name()).isEqualTo("metrics");
        assertThat(pool.queue()).isEqualTo(QUEUE);
        assertThat(pool.concurrency()).isEqualTo(4);
        assertThat(pool.busy()).isZero();
        assertThat(pool.utilization()).isZero();
        assertThat(pool.busyNanos()).isZero();

        long submitted = System.nanoTime();
        for (int i = 0; i < 3; i++) submit(QUEUE, "block", "{}", 3, LONG_TIMEOUT);
        for (int i = 0; i < 3; i++) handler.awaitStart();
        long allStarted = System.nanoTime();
        assertThat(pool.busy()).isEqualTo(3);
        assertThat(pool.utilization()).isEqualTo(0.75);

        long released = System.nanoTime();
        handler.release(3);
        Await.until("all attempts finished", AWAIT, () -> pool.busy() == 0);
        long finished = System.nanoTime();
        assertThat(pool.utilization()).isZero();
        assertThat(pool.busyNanos()).isPositive()
                .isGreaterThanOrEqualTo(3 * (released - allStarted))
                .isLessThanOrEqualTo(3 * (finished - submitted));
    }

    @Test
    void concurrencyLimitsHowManyAttemptsRunAtOnce() throws Exception {
        GateHandler handler = register(new GateHandler("block", false));
        WorkerPool pool = startPool("pair", QUEUE, 2, GRACE, handler);
        List<UUID> ids = IntStream.range(0, 5).mapToObj(i -> submit(QUEUE, "block", "{}", 3, LONG_TIMEOUT)).toList();

        handler.awaitStart();
        handler.awaitStart();
        assertThat(pool.busy()).isEqualTo(2);
        assertThat(count(ids, JobState.RUNNING)).isEqualTo(2);
        assertThat(count(ids, JobState.QUEUED)).isEqualTo(3);

        for (int finished = 1; finished <= 3; finished++) {
            handler.release(1);
            handler.awaitStart(); // the freed slot takes the next job
            int done = finished;
            Await.until(done + " jobs succeeded", AWAIT, () -> count(ids, JobState.SUCCEEDED) == done);
            assertThat(handler.active).hasValue(2);
            assertThat(count(ids, JobState.RUNNING)).isEqualTo(2);
            assertThat(count(ids, JobState.QUEUED)).isEqualTo(3 - finished);
        }
        handler.release(2);
        Await.until("all jobs succeeded", AWAIT, () -> count(ids, JobState.SUCCEEDED) == 5);
        assertThat(handler.maxActive).hasValue(2);
        assertThat(handler.started).as("exactly five attempts").isEmpty();
    }

    @Test
    void poolsOnlyTakeJobsFromTheirOwnQueue() {
        GateHandler alphaHandler = register(new GateHandler("block", false));
        GateHandler betaHandler = register(new GateHandler("block", false));
        alphaHandler.releaseAll();
        betaHandler.releaseAll();
        startPool("alpha-pool", "alpha", 2, GRACE, alphaHandler);
        startPool("beta-pool", "beta", 2, GRACE, betaHandler);
        List<UUID> alphaIds = IntStream.range(0, 3).mapToObj(i -> submit("alpha", "block", "{}", 3, LONG_TIMEOUT))
                .toList();
        List<UUID> betaIds = IntStream.range(0, 3).mapToObj(i -> submit("beta", "block", "{}", 3, LONG_TIMEOUT))
                .toList();

        alphaIds.forEach(id -> awaitState(id, JobState.SUCCEEDED));
        betaIds.forEach(id -> awaitState(id, JobState.SUCCEEDED));
        assertThat(alphaHandler.started).extracting(TaskContext::jobId).containsExactlyInAnyOrderElementsOf(alphaIds);
        assertThat(alphaHandler.started).extracting(TaskContext::queue).containsOnly("alpha");
        assertThat(betaHandler.started).extracting(TaskContext::jobId).containsExactlyInAnyOrderElementsOf(betaIds);
        assertThat(betaHandler.started).extracting(TaskContext::queue).containsOnly("beta");
        alphaIds.forEach(id -> assertThat(delivery(id).leaseOwner()).startsWith("alpha-pool@"));
        betaIds.forEach(id -> assertThat(delivery(id).leaseOwner()).startsWith("beta-pool@"));
    }

    @Test
    void settingsAndStartAreValidated() {
        assertThatThrownBy(() -> new WorkerPool.Settings("p", QUEUE, 0, HEARTBEAT, POLL, GRACE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new WorkerPool.Settings("p", QUEUE, 1, Duration.ZERO, POLL, GRACE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new WorkerPool.Settings("p", QUEUE, 1, HEARTBEAT.negated(), POLL, GRACE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new WorkerPool.Settings(null, QUEUE, 1, HEARTBEAT, POLL, GRACE))
                .isInstanceOf(NullPointerException.class);
        WorkerPool pool = startPool("once", QUEUE, 1, GRACE, new FnHandler("noop", c -> null));
        assertThatThrownBy(pool::start).isInstanceOf(IllegalStateException.class);
    }

    // ------------------------------------------------------------------------------------------ helpers

    private WorkerPool startPool(String name, String queue, int concurrency, Duration shutdownGrace,
                                 TaskHandler... handlers) {
        WorkerPool pool = new WorkerPool(new WorkerPool.Settings(name, queue, concurrency, HEARTBEAT, POLL,
                shutdownGrace), protocol, new TaskHandlerRegistry(List.of(handlers)), clock, mapper);
        pools.add(pool);
        pool.start();
        return pool;
    }

    private UUID submit(String queue, String type, String payload, int maxAttempts, Duration timeout) {
        return lifecycle.submit(new SubmitCommand(queue, type, payload, maxAttempts, timeout, null)).job().id();
    }

    private <T extends Releasable> T register(T handler) {
        releasables.add(handler);
        return handler;
    }

    private Job job(UUID id) {
        return repository.find(id).orElseThrow();
    }

    /** The latest delivery (attempt) of the job. */
    private Delivery delivery(UUID id) {
        List<Delivery> all = repository.deliveries(id);
        assertThat(all).as("deliveries of %s", id).isNotEmpty();
        return all.get(all.size() - 1);
    }

    private Job awaitState(UUID id, JobState state) {
        return Await.value("job " + id + " in state " + state, AWAIT, () -> job(id), j -> j.state() == state);
    }

    private long count(List<UUID> ids, JobState state) {
        return ids.stream().filter(id -> job(id).state() == state).count();
    }

    /**
     * Advances the clock by {@code step}, then waits for {@code condition}. The supervisor arms its first heartbeat
     * timer from the clock just after handing the attempt to the handler thread; if the step raced ahead of that,
     * the clock is nudged by one heartbeat interval every {@link #NUDGE_AFTER} until the condition holds.
     */
    private void advanceUntil(String description, Duration step, BooleanSupplier condition) {
        clock.advance(step);
        long[] nextNudge = {System.nanoTime() + NUDGE_AFTER.toNanos()};
        Await.until(description, AWAIT, () -> {
            if (condition.getAsBoolean()) return true;
            if (System.nanoTime() - nextNudge[0] > 0) {
                clock.advance(HEARTBEAT);
                nextNudge[0] = System.nanoTime() + NUDGE_AFTER.toNanos();
            }
            return false;
        });
    }

    /** stop() is inside its timed join on the slots, i.e. it already interrupted the slots it considers idle. */
    private static void awaitInsideStop(Thread stopper) {
        Await.until("stop() waiting for its slots", AWAIT, () -> stopper.getState() == Thread.State.TIMED_WAITING);
    }

    // ------------------------------------------------------------------------------------------ test doubles

    private interface Releasable {
        /** Lets every current and future attempt of this handler finish. */
        void releaseAll();
    }

    @FunctionalInterface
    private interface Script {
        Object run(TaskContext context) throws Exception;
    }

    /** Handler that runs a script. */
    private record FnHandler(String type, Script script) implements TaskHandler {
        @Override
        public Object handle(TaskContext context) throws Exception {
            return script.run(context);
        }
    }

    /**
     * Blocks every attempt until the test releases a permit, then returns {@code {"attempt": n}}. Records starts,
     * concurrent executions, interruptions and exits. With {@code returnWhenInterrupted} it swallows the interrupt
     * and returns a result anyway, like a handler that ignores cancellation.
     */
    private static final class GateHandler implements TaskHandler, Releasable {
        private final String type;
        private final boolean returnWhenInterrupted;
        private final Semaphore gate = new Semaphore(0);
        final BlockingQueue<TaskContext> started = new LinkedBlockingQueue<>();
        final AtomicInteger active = new AtomicInteger();
        final AtomicInteger maxActive = new AtomicInteger();
        final AtomicInteger interruptions = new AtomicInteger();
        final AtomicInteger exited = new AtomicInteger();

        GateHandler(String type, boolean returnWhenInterrupted) {
            this.type = type;
            this.returnWhenInterrupted = returnWhenInterrupted;
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public Object handle(TaskContext context) throws InterruptedException {
            maxActive.accumulateAndGet(active.incrementAndGet(), Math::max);
            try {
                started.add(context);
                try {
                    gate.acquire();
                } catch (InterruptedException e) {
                    interruptions.incrementAndGet();
                    if (!returnWhenInterrupted) throw e;
                }
                return Map.of("attempt", context.attempt());
            } finally {
                active.decrementAndGet();
                exited.incrementAndGet();
            }
        }

        /** Takes the next attempt start (in start order). */
        TaskContext awaitStart() throws InterruptedException {
            TaskContext context = started.poll(AWAIT.toMillis(), MILLISECONDS);
            assertThat(context).as("an attempt of %s started", type).isNotNull();
            return context;
        }

        void release(int permits) {
            gate.release(permits);
        }

        @Override
        public void releaseAll() {
            gate.release(10_000);
        }
    }

    /** Reports each progress value the test sends (acknowledging it) until it is released. */
    private static final class ProgressHandler implements TaskHandler, Releasable {
        private static final int FINISH = Integer.MIN_VALUE;
        private final BlockingQueue<Integer> steps = new LinkedBlockingQueue<>();
        private final BlockingQueue<Integer> applied = new LinkedBlockingQueue<>();
        final CountDownLatch started = new CountDownLatch(1);

        @Override
        public String type() {
            return "progress";
        }

        @Override
        public Object handle(TaskContext context) throws InterruptedException {
            started.countDown();
            while (true) {
                int step = steps.take();
                if (step == FINISH) return Map.of("done", true);
                context.reportProgress(step);
                applied.add(step);
            }
        }

        void report(int percent) throws InterruptedException {
            steps.add(percent);
            assertThat(applied.poll(AWAIT.toMillis(), MILLISECONDS)).isEqualTo(percent);
        }

        @Override
        public void releaseAll() {
            steps.add(FINISH);
        }
    }

    /** One call from the worker to the control plane: {@code kind} and its outcome. */
    private record Call(UUID jobId, String kind, String result) {
    }

    /**
     * Forwards to the real protocol and records every call. Can make heartbeats fail (control plane unreachable) and
     * pause one slot right after it obtained a lease, ignoring interrupts like a blocking database call does.
     */
    private static final class RecordingProtocol implements WorkerProtocol {
        private final WorkerProtocol delegate;
        private final List<Call> calls = new CopyOnWriteArrayList<>();
        private final AtomicBoolean pauseNextLease = new AtomicBoolean();
        private final CountDownLatch pausedWithLease = new CountDownLatch(1);
        private final CountDownLatch resume = new CountDownLatch(1);
        volatile boolean heartbeatsDown;

        RecordingProtocol(WorkerProtocol delegate) {
            this.delegate = delegate;
        }

        @Override
        public Optional<Lease> acquireNext(String workerId, String queue, Duration wait) throws InterruptedException {
            Optional<Lease> lease = delegate.acquireNext(workerId, queue, wait);
            if (lease.isPresent() && pauseNextLease.compareAndSet(true, false)) {
                pausedWithLease.countDown();
                boolean interrupted = false;
                while (true) {
                    try {
                        resume.await();
                        break;
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                if (interrupted) Thread.currentThread().interrupt();
            }
            return lease;
        }

        @Override
        public HeartbeatResult heartbeat(Lease lease, Integer progress) {
            if (heartbeatsDown) {
                calls.add(new Call(lease.job().id(), "heartbeat", "UNREACHABLE"));
                throw new IllegalStateException("control plane unreachable");
            }
            HeartbeatResult result = delegate.heartbeat(lease, progress);
            calls.add(new Call(lease.job().id(), "heartbeat", result.status().name()));
            return result;
        }

        @Override
        public Outcome complete(Lease lease, String resultJson) {
            Outcome outcome = delegate.complete(lease, resultJson);
            calls.add(new Call(lease.job().id(), "complete", outcome.name()));
            return outcome;
        }

        @Override
        public Outcome fail(Lease lease, TaskFailure failure) {
            Outcome outcome = delegate.fail(lease, failure);
            calls.add(new Call(lease.job().id(), "fail", failure.kind() + " " + outcome));
            return outcome;
        }

        @Override
        public void acknowledgeCancel(Lease lease) {
            delegate.acknowledgeCancel(lease);
            calls.add(new Call(lease.job().id(), "acknowledgeCancel", ""));
        }

        /** Every call except heartbeats, in order. */
        List<Call> reports(UUID jobId) {
            return calls.stream().filter(c -> c.jobId().equals(jobId) && !c.kind().equals("heartbeat")).toList();
        }

        /** Outcomes of the heartbeats, in order. */
        List<String> heartbeats(UUID jobId) {
            return calls.stream().filter(c -> c.jobId().equals(jobId) && c.kind().equals("heartbeat"))
                    .map(Call::result).toList();
        }

        void pauseNextLease() {
            pauseNextLease.set(true);
        }

        void awaitPausedWithLease() throws InterruptedException {
            assertThat(pausedWithLease.await(AWAIT.toMillis(), MILLISECONDS)).as("a slot leased a job").isTrue();
        }

        void resumeAfterLease() {
            resume.countDown();
        }
    }

    /** Remembers the job snapshot after every finished attempt and cancellation. */
    private static final class RecordingListener implements LifecycleListener {
        private final List<Job> snapshots = new CopyOnWriteArrayList<>();

        @Override
        public void onAttemptFinished(Job next, Delivery closed) {
            snapshots.add(next);
        }

        @Override
        public void onCancelled(Job cancelled, JobState from) {
            snapshots.add(cancelled);
        }

        List<JobState> states(UUID id) {
            return snapshots.stream().filter(j -> j.id().equals(id)).map(Job::state).toList();
        }
    }
}
