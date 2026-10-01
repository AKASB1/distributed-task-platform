package io.akasb.taskplatform.dispatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.akasb.taskplatform.domain.RetryPolicy;
import io.akasb.taskplatform.observability.LifecycleListener;
import io.akasb.taskplatform.persistence.InMemoryJobRepository;
import io.akasb.taskplatform.support.MutableClock;
import io.akasb.taskplatform.worker.LocalWorkerProtocol;
import io.akasb.taskplatform.worker.TaskHandlerRegistry;
import io.akasb.taskplatform.worker.WorkerPool;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Startup recovery beyond one batch, and the start/stop rules of a worker pool. */
class StartupRecoveryAndPoolLifecycleTest {
    private final MutableClock clock = MutableClock.startingAt("2026-01-01T00:00:00Z");
    private final InMemoryJobRepository repository = new InMemoryJobRepository();

    private JobLifecycle lifecycle(JobDispatcher dispatcher) {
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(1), 2.0, Duration.ofMinutes(1), 0.5, new Random(5));
        return new JobLifecycle(repository, dispatcher, retry, clock, Duration.ofSeconds(30), LifecycleListener.none(),
                Checkpoints.none());
    }

    @Test
    void startupRecoveryRedispatchesEveryQueuedJobEvenBeyondOneBatch() throws InterruptedException {
        JobLifecycle before = lifecycle(new InMemoryDispatcher());
        Set<UUID> queued = new HashSet<>();
        for (int i = 0; i < 23; i++) {
            queued.add(before.submit(new SubmitCommand("q", "sleep", "{}", 3, Duration.ofSeconds(5), null)).job().id());
        }
        clock.advance(Duration.ofSeconds(1));

        // restart: a new, empty in-memory queue; batch size 5 means five batches are needed
        InMemoryDispatcher dispatcher = new InMemoryDispatcher();
        Reconciler reconciler = new Reconciler(repository, lifecycle(dispatcher), dispatcher,
                new Reconciler.Settings(Duration.ofSeconds(2), Duration.ofSeconds(5), Duration.ofSeconds(60), 5));
        assertThat(reconciler.recoverOnStartup().redispatched()).isEqualTo(23);

        Set<UUID> delivered = new HashSet<>();
        for (var id = dispatcher.poll("q", Duration.ZERO); id.isPresent(); id = dispatcher.poll("q", Duration.ZERO)) {
            delivered.add(id.get());
        }
        assertThat(delivered).isEqualTo(queued);
        assertThat(reconciler.reconcileOnce().redispatched()).as("nothing left to re-dispatch").isZero();
    }

    @Test
    void aStoppedPoolCannotBeRestartedAndSettingsRejectNonPositivePollTimeouts() {
        InMemoryDispatcher dispatcher = new InMemoryDispatcher();
        WorkerPool pool = new WorkerPool(new WorkerPool.Settings("p", "q", 1, Duration.ofMillis(20),
                Duration.ofMillis(20), Duration.ZERO), new LocalWorkerProtocol(dispatcher, lifecycle(dispatcher)),
                new TaskHandlerRegistry(List.of()), clock, new ObjectMapper());
        pool.start();
        pool.stop();
        assertThatThrownBy(pool::start).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> new WorkerPool.Settings("p", "q", 1, Duration.ofMillis(20), Duration.ZERO,
                Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new WorkerPool.Settings("p", "q", 1, Duration.ofMillis(20), Duration.ofMillis(5),
                Duration.ofSeconds(-1))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void stoppingSeveralPoolsSharesOneGracePeriod() {
        InMemoryDispatcher dispatcher = new InMemoryDispatcher();
        JobLifecycle lifecycle = lifecycle(dispatcher);
        List<WorkerPool> pools = List.of("a", "b", "c").stream().map(name -> new WorkerPool(
                new WorkerPool.Settings(name, name, 2, Duration.ofMillis(20), Duration.ofSeconds(5),
                        Duration.ofSeconds(5)), new LocalWorkerProtocol(dispatcher, lifecycle),
                new TaskHandlerRegistry(List.of()), clock, new ObjectMapper())).toList();
        pools.forEach(WorkerPool::start);
        long started = System.nanoTime();
        pools.forEach(WorkerPool::beginStop);
        pools.forEach(WorkerPool::awaitStop);
        // idle slots are interrupted at once, so stopping does not wait for a poll timeout or a grace period per pool
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(4));
        pools.forEach(p -> assertThatThrownBy(p::start).isInstanceOf(IllegalStateException.class));
    }
}
