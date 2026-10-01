package io.akasb.taskplatform.dispatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import com.zaxxer.hikari.HikariDataSource;
import io.akasb.taskplatform.domain.AckState;
import io.akasb.taskplatform.domain.Delivery;
import io.akasb.taskplatform.domain.FailureKind;
import io.akasb.taskplatform.domain.Job;
import io.akasb.taskplatform.domain.JobState;
import io.akasb.taskplatform.domain.Lease;
import io.akasb.taskplatform.domain.RetryPolicy;
import io.akasb.taskplatform.domain.TaskFailure;
import io.akasb.taskplatform.observability.LifecycleListener;
import io.akasb.taskplatform.persistence.JdbcJobRepository;
import io.akasb.taskplatform.persistence.JobRepository;
import io.akasb.taskplatform.support.EmbeddedPostgresSupport;
import io.akasb.taskplatform.support.MutableClock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Concurrency races on real PostgreSQL: competing claims, duplicate completions, complete against cancel, a zombie
 * worker after a lease reclaim, two reconcilers over the same expired leases, and a heartbeat against lease expiry.
 * Every contender is released by a {@link CyclicBarrier}; the database compare-and-set must pick exactly one winner.
 */
class RaceIT {
    private static final String QUEUE = "race";
    private static final Duration LEASE = Duration.ofSeconds(30);
    private static final Reconciler.Settings RECONCILER = new Reconciler.Settings(Duration.ofSeconds(5),
            Duration.ofSeconds(10), Duration.ofSeconds(30), 500);
    private static final int THREADS = 8;
    private static final long TIMEOUT_S = 30;

    private HikariDataSource dataSource;
    private JobRepository repository;
    private InMemoryDispatcher dispatcher;
    private MutableClock clock;
    private RetryPolicy retryPolicy;
    private CountingListener listener;
    private JobLifecycle lifecycle;
    private Reconciler reconciler;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        dataSource = EmbeddedPostgresSupport.newMigratedDataSource(32);
        repository = new JdbcJobRepository(dataSource);
        dispatcher = new InMemoryDispatcher();
        clock = MutableClock.startingAt("2026-01-01T00:00:00Z");
        retryPolicy = new RetryPolicy(Duration.ofSeconds(1), 2.0, Duration.ofMinutes(1), 0.5, new Random(42));
        listener = new CountingListener();
        lifecycle = new JobLifecycle(repository, dispatcher, retryPolicy, clock, LEASE, listener, Checkpoints.none());
        reconciler = new Reconciler(repository, lifecycle, dispatcher, RECONCILER);
        executor = Executors.newFixedThreadPool(THREADS);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        try {
            executor.shutdownNow();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } finally {
            dataSource.close();
        }
    }

    @Test
    void concurrentClaimsOfTheSameJobYieldExactlyOneLease() throws Exception {
        for (int n = 0; n < 50; n++) {
            Job queued = submit();
            CyclicBarrier start = new CyclicBarrier(THREADS);
            List<Callable<Optional<Lease>>> contenders = new ArrayList<>();
            for (int i = 0; i < THREADS; i++) {
                String worker = "w" + i;
                contenders.add(() -> {
                    start.await(TIMEOUT_S, TimeUnit.SECONDS);
                    return lifecycle.claim(queued.id(), worker);
                });
            }
            List<Lease> leases = runAll(contenders).stream().flatMap(Optional::stream).toList();

            assertThat(leases).as("leases for job #%d", n).hasSize(1);
            Lease lease = leases.get(0);
            Job stored = repository.find(queued.id()).orElseThrow();
            assertThat(stored.state()).isEqualTo(JobState.RUNNING);
            assertThat(stored.attempts()).isEqualTo(1);
            assertThat(stored.version()).isEqualTo(queued.version() + 1).isEqualTo(lease.job().version());
            List<Delivery> deliveries = repository.deliveries(queued.id());
            assertThat(deliveries).hasSize(1);
            assertThat(deliveries.get(0).attempt()).isEqualTo(1);
            assertThat(deliveries.get(0).ackState()).isEqualTo(AckState.LEASED);
            assertThat(deliveries.get(0).leaseOwner()).isEqualTo(lease.owner());
            assertThat(listener.count("claimed", queued.id())).isEqualTo(1);
        }
    }

    @Test
    void duplicateCompletionsWithTheSameLeaseAreAcceptedOnce() throws Exception {
        Job queued = submit();
        Lease lease = lifecycle.claim(queued.id(), "w").orElseThrow();
        CyclicBarrier start = new CyclicBarrier(THREADS);
        List<Callable<Outcome>> reports = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            String result = "{\"reporter\":" + i + "}";
            reports.add(() -> {
                start.await(TIMEOUT_S, TimeUnit.SECONDS);
                return lifecycle.complete(lease, result);
            });
        }
        List<Outcome> outcomes = runAll(reports);

        assertThat(outcomes).filteredOn(o -> o == Outcome.ACCEPTED).hasSize(1);
        assertThat(outcomes).filteredOn(o -> o != Outcome.ACCEPTED).hasSize(THREADS - 1)
                .containsOnly(Outcome.LEASE_LOST);
        Job stored = repository.find(queued.id()).orElseThrow();
        assertThat(stored.state()).isEqualTo(JobState.SUCCEEDED);
        assertThat(stored.version()).as("succeeded exactly once").isEqualTo(lease.job().version() + 1);
        assertThat(stored.result()).isEqualTo(
                "{\"reporter\": " + outcomes.indexOf(Outcome.ACCEPTED) + "}");
        List<Delivery> deliveries = repository.deliveries(queued.id());
        assertThat(deliveries).singleElement().satisfies(d -> {
            assertThat(d.ackState()).isEqualTo(AckState.ACKED);
            assertThat(d.progress()).isEqualTo(100);
        });
        assertThat(listener.count("acked", queued.id())).isEqualTo(1);
    }

    @Test
    void completeRacingCancelHasExactlyOneWinner() throws Exception {
        // Both orderings once without a race, so each branch is checked on every run.
        Job first = submit();
        Lease firstLease = lifecycle.claim(first.id(), "w").orElseThrow();
        Outcome early = lifecycle.complete(firstLease, "{\"winner\":\"complete\"}");
        assertThat(checkCompleteCancel(-1, first, firstLease, early, cancelOrConflict(first.id())))
                .isEqualTo(JobState.SUCCEEDED);
        Job second = submit();
        Lease secondLease = lifecycle.claim(second.id(), "w").orElseThrow();
        Object cancelFirst = cancelOrConflict(second.id());
        assertThat(checkCompleteCancel(-2, second, secondLease,
                lifecycle.complete(secondLease, "{\"winner\":\"complete\"}"), cancelFirst))
                .isEqualTo(JobState.CANCELLED);

        Map<JobState, Integer> winners = new EnumMap<>(JobState.class);
        for (int n = 0; n < 50; n++) {
            Job queued = submit();
            Lease lease = lifecycle.claim(queued.id(), "w").orElseThrow();
            boolean rereadFirst = n % 2 == 1; // a worker that re-reads before reporting: evens out who wins
            CyclicBarrier start = new CyclicBarrier(2);
            Future<Outcome> completion = executor.submit(() -> {
                start.await(TIMEOUT_S, TimeUnit.SECONDS);
                if (rereadFirst) repository.find(queued.id());
                return lifecycle.complete(lease, "{\"winner\":\"complete\"}");
            });
            Future<Object> cancellation = executor.submit(() -> {
                start.await(TIMEOUT_S, TimeUnit.SECONDS);
                return cancelOrConflict(queued.id());
            });
            Outcome outcome = completion.get(TIMEOUT_S, TimeUnit.SECONDS);
            Object cancelResult = cancellation.get(TIMEOUT_S, TimeUnit.SECONDS);
            winners.merge(checkCompleteCancel(n, queued, lease, outcome, cancelResult), 1, Integer::sum);
        }
        assertThat(winners.values().stream().mapToInt(Integer::intValue).sum()).isEqualTo(50);
        assertThat(winners.keySet()).isSubsetOf(JobState.SUCCEEDED, JobState.CANCELLED);
        System.out.printf("complete-vs-cancel: %s%n", winners);
    }

    @Test
    void zombieWorkerCannotReportAfterItsLeaseWasReclaimed() {
        Job queued = submit();
        Lease leaseA = lifecycle.claim(queued.id(), "worker-A").orElseThrow();

        clock.advance(LEASE.plus(RECONCILER.leaseGrace()).plusMillis(1));
        Reconciler.Report reclaimed = reconciler.reconcileOnce();
        assertThat(reclaimed.leasesExpired()).isEqualTo(1);
        Job waiting = repository.find(queued.id()).orElseThrow();
        assertThat(waiting.state()).isEqualTo(JobState.RETRY_WAIT);
        assertThat(waiting.lastError()).startsWith(FailureKind.LEASE_EXPIRED.name());
        assertThat(lifecycle.heartbeat(leaseA, 10).status()).isEqualTo(HeartbeatResult.Status.LOST);

        clock.advance(retryPolicy.baseDelay(1));
        assertThat(reconciler.reconcileOnce().retriesReleased()).isEqualTo(1);
        Lease leaseB = lifecycle.claim(queued.id(), "worker-B").orElseThrow();
        assertThat(leaseB.attempt()).isEqualTo(2);

        assertThat(lifecycle.complete(leaseA, "{\"by\":\"A\"}")).isEqualTo(Outcome.LEASE_LOST);
        assertThat(lifecycle.fail(leaseA, new TaskFailure(FailureKind.RETRYABLE, "late")))
                .isEqualTo(Outcome.LEASE_LOST);
        assertThat(lifecycle.heartbeat(leaseA, 90).status()).isEqualTo(HeartbeatResult.Status.LOST);
        HeartbeatResult renewed = lifecycle.heartbeat(leaseB, 50);
        assertThat(renewed.status()).isEqualTo(HeartbeatResult.Status.RENEWED);

        assertThat(lifecycle.complete(renewed.lease(), "{\"by\":\"B\"}")).isEqualTo(Outcome.ACCEPTED);
        assertThat(lifecycle.complete(leaseA, "{\"by\":\"A\"}")).isEqualTo(Outcome.LEASE_LOST);

        Job done = repository.find(queued.id()).orElseThrow();
        assertThat(done.state()).isEqualTo(JobState.SUCCEEDED);
        assertThat(done.attempts()).isEqualTo(2);
        assertThat(done.result()).isEqualTo("{\"by\": \"B\"}");
        List<Delivery> deliveries = repository.deliveries(queued.id());
        assertThat(deliveries).extracting(Delivery::attempt, Delivery::leaseOwner, Delivery::ackState)
                .containsExactly(
                        tuple(1, "worker-A", AckState.EXPIRED),
                        tuple(2, "worker-B", AckState.ACKED));
        assertThat(deliveries).filteredOn(d -> d.ackState() == AckState.ACKED).hasSize(1);
        assertThat(listener.count("acked", queued.id())).isEqualTo(1);
    }

    @Test
    void concurrentReconcilersExpireEachLeaseExactlyOnce() throws Exception {
        int jobs = 50;
        List<Lease> leases = new ArrayList<>();
        for (int i = 0; i < jobs; i++) {
            Job queued = submit();
            leases.add(lifecycle.claim(queued.id(), "w" + i).orElseThrow());
        }
        clock.advance(LEASE.plus(RECONCILER.leaseGrace()).plusMillis(1));

        List<Reconciler.Report> expiry = runConcurrently(2, reconciler::reconcileOnce);
        System.out.printf("leases expired per reconciler: %d / %d%n", expiry.get(0).leasesExpired(),
                expiry.get(1).leasesExpired());
        assertThat(expiry.stream().mapToInt(Reconciler.Report::leasesExpired).sum()).isEqualTo(jobs);
        assertThat(expiry).allSatisfy(r -> assertThat(r.retriesReleased()).isZero());
        for (Lease lease : leases) {
            UUID id = lease.job().id();
            Job stored = repository.find(id).orElseThrow();
            assertThat(stored.state()).isEqualTo(JobState.RETRY_WAIT);
            assertThat(stored.attempts()).isEqualTo(1);
            assertThat(stored.version()).isEqualTo(lease.job().version() + 1);
            assertThat(repository.deliveries(id)).singleElement().satisfies(d -> {
                assertThat(d.attempt()).isEqualTo(1);
                assertThat(d.ackState()).isEqualTo(AckState.EXPIRED);
            });
            assertThat(listener.count("leaseExpired", id)).as("expiries of %s", id).isEqualTo(1);
            assertThat(listener.count("retryScheduled", id)).as("retries of %s", id).isEqualTo(1);
        }

        // Both reconcilers release the due retries: again every job exactly once.
        clock.advance(retryPolicy.baseDelay(1));
        List<Reconciler.Report> release = runConcurrently(2, reconciler::reconcileOnce);
        System.out.printf("retries released per reconciler: %d / %d%n", release.get(0).retriesReleased(),
                release.get(1).retriesReleased());
        assertThat(release.stream().mapToInt(Reconciler.Report::retriesReleased).sum()).isEqualTo(jobs);
        assertThat(release).allSatisfy(r -> assertThat(r.leasesExpired()).isZero());
        for (Lease lease : leases) {
            UUID id = lease.job().id();
            Job stored = repository.find(id).orElseThrow();
            assertThat(stored.state()).isEqualTo(JobState.QUEUED);
            assertThat(stored.attempts()).isEqualTo(1);
            assertThat(stored.version()).isEqualTo(lease.job().version() + 2);
            assertThat(listener.count("retryReleased", id)).isEqualTo(1);
            Lease second = lifecycle.claim(id, "again").orElseThrow();
            assertThat(second.attempt()).isEqualTo(2);
            assertThat(repository.deliveries(id)).extracting(Delivery::attempt, Delivery::ackState)
                    .containsExactly(tuple(1, AckState.EXPIRED),
                            tuple(2, AckState.LEASED));
        }
        assertThat(repository.countByState()).containsExactly(Map.entry(JobState.RUNNING, (long) jobs));
    }

    @Test
    void heartbeatRacingLeaseExpiryHasExactlyOneWinner() throws Exception {
        Map<HeartbeatResult.Status, Integer> winners = new EnumMap<>(HeartbeatResult.Status.class);
        for (int n = 0; n < 30; n++) {
            Job queued = submit();
            Lease lease = lifecycle.claim(queued.id(), "w").orElseThrow();
            clock.advance(LEASE.plus(RECONCILER.leaseGrace()).plusMillis(1));
            CyclicBarrier start = new CyclicBarrier(2);
            Future<HeartbeatResult> heartbeat = executor.submit(() -> {
                start.await(TIMEOUT_S, TimeUnit.SECONDS);
                return lifecycle.heartbeat(lease, 50);
            });
            Future<Reconciler.Report> pass = executor.submit(() -> {
                start.await(TIMEOUT_S, TimeUnit.SECONDS);
                return reconciler.reconcileOnce();
            });
            HeartbeatResult hb = heartbeat.get(TIMEOUT_S, TimeUnit.SECONDS);
            Reconciler.Report report = pass.get(TIMEOUT_S, TimeUnit.SECONDS);
            Job stored = repository.find(queued.id()).orElseThrow();
            Delivery delivery = repository.deliveries(queued.id()).get(0);
            winners.merge(hb.status(), 1, Integer::sum);

            if (hb.status() == HeartbeatResult.Status.RENEWED) {
                assertThat(report.leasesExpired()).as("iteration %d", n).isZero();
                assertThat(stored.state()).isEqualTo(JobState.RUNNING);
                assertThat(stored.version()).isEqualTo(lease.job().version());
                assertThat(delivery.ackState()).isEqualTo(AckState.LEASED);
                assertThat(delivery.leaseDeadline()).isEqualTo(hb.lease().deadline());
                assertThat(lifecycle.complete(hb.lease(), null)).isEqualTo(Outcome.ACCEPTED);
            } else {
                assertThat(hb.status()).as("iteration %d", n).isEqualTo(HeartbeatResult.Status.LOST);
                assertThat(report.leasesExpired()).isEqualTo(1);
                assertThat(stored.state()).isEqualTo(JobState.RETRY_WAIT);
                assertThat(stored.version()).isEqualTo(lease.job().version() + 1);
                assertThat(delivery.ackState()).isEqualTo(AckState.EXPIRED);
                assertThat(lifecycle.cancel(queued.id()).state()).isEqualTo(JobState.CANCELLED);
            }
            assertThat(repository.deliveries(queued.id())).hasSize(1);
        }
        System.out.printf("heartbeat-vs-expiry: %s%n", winners);
    }

    // ------------------------------------------------------------------------------------------ helpers

    private Job submit() {
        SubmitResult result = lifecycle.submit(new SubmitCommand(QUEUE, "work", "{}", 3, Duration.ofMinutes(10),
                null));
        assertThat(result.job().state()).isEqualTo(JobState.QUEUED);
        return result.job();
    }

    private Object cancelOrConflict(UUID id) {
        try {
            return lifecycle.cancel(id);
        } catch (JobStateConflictException e) {
            return e;
        }
    }

    /** Asserts that exactly one of complete / cancel won and returns the final state. */
    private JobState checkCompleteCancel(int n, Job queued, Lease lease, Outcome outcome, Object cancelResult) {
        Job stored = repository.find(queued.id()).orElseThrow();
        List<Delivery> deliveries = repository.deliveries(queued.id());
        assertThat(stored.version()).as("iteration %d: one change", n).isEqualTo(lease.job().version() + 1);
        if (stored.state() == JobState.SUCCEEDED) {
            assertThat(outcome).as("iteration %d", n).isEqualTo(Outcome.ACCEPTED);
            assertThat(cancelResult).as("iteration %d", n).isInstanceOf(JobStateConflictException.class);
            assertThat(((JobStateConflictException) cancelResult).job().state()).isEqualTo(JobState.SUCCEEDED);
            assertThat(deliveries).singleElement().extracting(Delivery::ackState).isEqualTo(AckState.ACKED);
            assertThat(listener.count("cancelled", queued.id())).isZero();
            assertThat(listener.count("acked", queued.id())).isEqualTo(1);
            return stored.state();
        }
        assertThat(stored.state()).as("iteration %d", n).isEqualTo(JobState.CANCELLED);
        assertThat(outcome).as("iteration %d", n).isEqualTo(Outcome.CANCELLED);
        assertThat(cancelResult).as("iteration %d", n).isInstanceOf(Job.class);
        assertThat(((Job) cancelResult).state()).isEqualTo(JobState.CANCELLED);
        assertThat(listener.count("acked", queued.id())).isZero();
        assertThat(listener.count("cancelled", queued.id())).isEqualTo(1);
        // The rejected completion told the worker about the cancel, so the attempt is already closed as CANCELLED;
        // a later heartbeat still reports CANCELLED and a repeated acknowledgement is a no-op.
        assertThat(deliveries).singleElement().extracting(Delivery::ackState).isEqualTo(AckState.CANCELLED);
        assertThat(lifecycle.heartbeat(lease, 50).status()).isEqualTo(HeartbeatResult.Status.CANCELLED);
        lifecycle.acknowledgeCancel(lease);
        assertThat(repository.deliveries(queued.id())).singleElement().extracting(Delivery::ackState)
                .isEqualTo(AckState.CANCELLED);
        // A late completion can never resurrect a cancelled job.
        assertThat(lifecycle.complete(lease, "{}")).isEqualTo(Outcome.CANCELLED);
        assertThat(repository.find(queued.id()).orElseThrow())
                .extracting(Job::state, Job::version).containsExactly(JobState.CANCELLED, stored.version());
        return stored.state();
    }

    private <T> List<T> runAll(List<Callable<T>> tasks) throws Exception {
        List<Future<T>> futures = new ArrayList<>();
        for (Callable<T> task : tasks) futures.add(executor.submit(task));
        List<T> results = new ArrayList<>();
        for (Future<T> f : futures) results.add(f.get(TIMEOUT_S, TimeUnit.SECONDS));
        return results;
    }

    private <T> List<T> runConcurrently(int parties, Callable<T> task) throws Exception {
        CyclicBarrier start = new CyclicBarrier(parties);
        List<Callable<T>> tasks = new ArrayList<>();
        for (int i = 0; i < parties; i++) {
            tasks.add(() -> {
                start.await(TIMEOUT_S, TimeUnit.SECONDS);
                return task.call();
            });
        }
        return runAll(tasks);
    }

    /** Counts lifecycle events per job so "exactly once" can be asserted. */
    private static final class CountingListener implements LifecycleListener {
        private final Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();

        int count(String event, UUID jobId) {
            AtomicInteger c = counts.get(event + ":" + jobId);
            return c == null ? 0 : c.get();
        }

        private void inc(String event, UUID jobId) {
            counts.computeIfAbsent(event + ":" + jobId, k -> new AtomicInteger()).incrementAndGet();
        }

        @Override
        public void onClaimed(Job running, Delivery delivery) {
            inc("claimed", running.id());
        }

        @Override
        public void onAttemptFinished(Job next, Delivery closed) {
            if (closed.ackState() == AckState.ACKED) inc("acked", next.id());
        }

        @Override
        public void onRetryScheduled(Job next, FailureKind reason, Duration delay) {
            inc("retryScheduled", next.id());
        }

        @Override
        public void onLeaseExpired(Job running, Delivery expired) {
            inc("leaseExpired", running.id());
        }

        @Override
        public void onCancelled(Job cancelled, JobState from) {
            inc("cancelled", cancelled.id());
        }

        @Override
        public void onRecovered(Recovery kind, Job job) {
            if (kind == Recovery.RETRY_RELEASED) inc("retryReleased", job.id());
        }
    }
}
