package io.akasb.taskplatform.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.akasb.taskplatform.dispatch.Checkpoints;
import io.akasb.taskplatform.dispatch.InMemoryDispatcher;
import io.akasb.taskplatform.dispatch.JobLifecycle;
import io.akasb.taskplatform.domain.JobState;
import io.akasb.taskplatform.domain.RetryPolicy;
import io.akasb.taskplatform.observability.LifecycleListener;
import io.akasb.taskplatform.persistence.InMemoryJobRepository;
import io.akasb.taskplatform.persistence.JobRepository;
import io.akasb.taskplatform.support.MutableClock;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * {@link JobService} with an idempotency cache: a cache hit is only used after the database confirms it, and every
 * miss, stale entry or cache failure takes the authoritative database path. The cache can never create, hide or
 * change a job.
 */
class JobServiceIdempotencyCacheTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicInteger inserts = new AtomicInteger();
    private final FakeCache cache = new FakeCache();
    private InMemoryDispatcher dispatcher;
    private InMemoryJobRepository store;
    private JobService service;

    @BeforeEach
    void setUp() {
        dispatcher = new InMemoryDispatcher();
        store = new InMemoryJobRepository();
        JobRepository repository = countingInserts(store, inserts);
        MutableClock clock = MutableClock.startingAt("2026-01-01T00:00:00Z");
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(1), 2.0, Duration.ofMinutes(1), 0.5, new Random(1));
        JobLifecycle lifecycle = new JobLifecycle(repository, dispatcher, retry, clock, Duration.ofSeconds(30),
                LifecycleListener.none(), Checkpoints.none());
        service = new JobService(lifecycle, repository, () -> Set.of("sleep"), () -> Set.of("default"),
                new JobService.Limits(3, 20, Duration.ofSeconds(30), Duration.ofHours(1), 1024), mapper, cache);
    }

    private SubmitJobRequest sleep(long ms) {
        return new SubmitJobRequest("sleep", null, mapper.createObjectNode().put("durationMs", ms), null, null);
    }

    private long totalJobs() {
        return store.countByState().values().stream().mapToLong(Long::longValue).sum();
    }

    @Test
    void firstSubmissionTakesTheDatabasePathAndIsRemembered() {
        JobService.SubmitOutcome created = service.submit(sleep(10), "k");

        assertThat(created.created()).isTrue();
        assertThat(cache.lookups).hasValue(1);
        assertThat(inserts).hasValue(1);
        assertThat(cache.remembered).containsExactly(Map.entry("k", created.job().id()));
    }

    @Test
    void hitConfirmedByTheDatabaseIsAReplayWithoutInsert() throws InterruptedException {
        UUID id = service.submit(sleep(10), "k").job().id();
        assertThat(dispatcher.poll("default", Duration.ZERO)).contains(id);

        JobService.SubmitOutcome replay = service.submit(sleep(10), "k");

        assertThat(replay.created()).isFalse();
        assertThat(replay.job().id()).isEqualTo(id);
        assertThat(inserts).as("the database path was not taken").hasValue(1);
        assertThat(dispatcher.poll("default", Duration.ZERO)).as("nothing dispatched again").isEmpty();
        assertThat(totalJobs()).isEqualTo(1);
    }

    @Test
    void hitReturnsTheCurrentStoredJobNotACachedCopy() {
        UUID id = service.submit(sleep(10), "k").job().id();
        service.cancel(id);

        JobService.SubmitOutcome replay = service.submit(sleep(10), "k");

        assertThat(replay.created()).isFalse();
        assertThat(replay.job().state()).isEqualTo(JobState.CANCELLED);
        assertThat(replay.job().version()).isEqualTo(store.find(id).orElseThrow().version());
    }

    @Test
    void hitWithADifferentRequestIsStillAConflict() {
        UUID id = service.submit(sleep(10), "k").job().id();

        assertThatThrownBy(() -> service.submit(sleep(11), "k"))
                .isInstanceOf(IdempotencyConflictException.class)
                .satisfies(e -> assertThat(((IdempotencyConflictException) e).existingJobId()).isEqualTo(id));
        assertThat(inserts).hasValue(1);
        assertThat(totalJobs()).isEqualTo(1);
    }

    @Test
    void conflictIsStillDetectedOnTheDatabasePathAfterACacheMiss() {
        UUID id = service.submit(sleep(10), "k").job().id();
        cache.entries.clear(); // expired or evicted

        assertThatThrownBy(() -> service.submit(sleep(11), "k"))
                .isInstanceOf(IdempotencyConflictException.class)
                .satisfies(e -> assertThat(((IdempotencyConflictException) e).existingJobId()).isEqualTo(id));
        assertThat(inserts).hasValue(2);
        assertThat(cache.entries).containsEntry("k", id);
        assertThat(totalJobs()).isEqualTo(1);
    }

    @Test
    void staleEntryPointingToAJobWithAnotherKeyIsIgnored() {
        UUID other = service.submit(sleep(1), "a").job().id();
        cache.entries.put("b", other);

        JobService.SubmitOutcome created = service.submit(sleep(1), "b");

        assertThat(created.created()).isTrue();
        assertThat(created.job().id()).isNotEqualTo(other);
        assertThat(created.job().idempotencyKey()).isEqualTo("b");
        assertThat(cache.entries).as("stale entry replaced by the database answer")
                .containsEntry("b", created.job().id());
        assertThat(totalJobs()).isEqualTo(2);
    }

    @Test
    void staleEntryCannotHideTheJobThatOwnsTheKey() {
        UUID a = service.submit(sleep(1), "a").job().id();
        UUID b = service.submit(sleep(2), "b").job().id();
        cache.entries.put("b", a); // points at a job with a different key and a different request

        JobService.SubmitOutcome replay = service.submit(sleep(2), "b");

        assertThat(replay.created()).isFalse();
        assertThat(replay.job().id()).isEqualTo(b);
        assertThat(cache.entries).containsEntry("b", b);
        assertThat(totalJobs()).isEqualTo(2);
    }

    @Test
    void staleEntryPointingToAMissingJobIsIgnored() {
        UUID missing = UUID.fromString("00000000-0000-0000-0000-000000000001");
        cache.entries.put("k", missing);

        JobService.SubmitOutcome created = service.submit(sleep(1), "k");

        assertThat(created.created()).isTrue();
        assertThat(created.job().id()).isNotEqualTo(missing);
        assertThat(store.find(missing)).isEmpty();
        assertThat(cache.entries).containsEntry("k", created.job().id());
    }

    @Test
    void failingLookupFallsBackToTheDatabase() {
        cache.failFind = new IllegalStateException("redis down");

        JobService.SubmitOutcome first = service.submit(sleep(1), "k");
        JobService.SubmitOutcome replay = service.submit(sleep(1), "k");

        assertThat(first.created()).isTrue();
        assertThat(replay.created()).isFalse();
        assertThat(replay.job().id()).isEqualTo(first.job().id());
        assertThat(inserts).as("both went to the database").hasValue(2);
        assertThat(totalJobs()).isEqualTo(1);
        assertThatThrownBy(() -> service.submit(sleep(2), "k")).isInstanceOf(IdempotencyConflictException.class);
    }

    @Test
    void failingUpdateDoesNotFailTheSubmission() {
        cache.failRemember = new IllegalStateException("redis down");

        JobService.SubmitOutcome first = service.submit(sleep(1), "k");
        JobService.SubmitOutcome replay = service.submit(sleep(1), "k");

        assertThat(first.created()).isTrue();
        assertThat(replay.created()).isFalse();
        assertThat(replay.job().id()).isEqualTo(first.job().id());
        assertThat(cache.entries).isEmpty();
    }

    @Test
    void rememberedAfterCreationAndAfterADatabaseReplay() {
        cache.store = false; // a cache that loses every entry: each lookup misses
        UUID id = service.submit(sleep(1), "k").job().id();
        JobService.SubmitOutcome replay = service.submit(sleep(1), "k");

        assertThat(replay.created()).isFalse();
        assertThat(cache.remembered).containsExactly(Map.entry("k", id), Map.entry("k", id));
        assertThat(cache.lookups).hasValue(2);
    }

    @Test
    void submissionsWithoutKeyNeverTouchTheCache() {
        service.submit(sleep(1), null);
        service.submit(sleep(1), null);

        assertThat(cache.lookups).hasValue(0);
        assertThat(cache.remembered).isEmpty();
        assertThat(totalJobs()).isEqualTo(2);
    }

    @Test
    void invalidRequestsAreRejectedBeforeTheCacheIsAsked() {
        assertThatThrownBy(() -> service.submit(sleep(1), "has space"))
                .isInstanceOf(InvalidJobRequestException.class);
        assertThat(cache.lookups).hasValue(0);
    }

    @Test
    void noneCacheAlwaysMisses() {
        IdempotencyCache none = IdempotencyCache.none();
        none.remember("k", UUID.randomUUID());
        assertThat(none.find("k")).isEmpty();
    }

    /** In-memory stand-in for Redis whose entries a test can poison or make fail. */
    static final class FakeCache implements IdempotencyCache {
        final Map<String, UUID> entries = new ConcurrentHashMap<>();
        final List<Map.Entry<String, UUID>> remembered = new CopyOnWriteArrayList<>();
        final AtomicInteger lookups = new AtomicInteger();
        volatile boolean store = true;
        volatile RuntimeException failFind;
        volatile RuntimeException failRemember;

        @Override
        public Optional<UUID> find(String key) {
            lookups.incrementAndGet();
            if (failFind != null) throw failFind;
            return Optional.ofNullable(entries.get(key));
        }

        @Override
        public void remember(String key, UUID jobId) {
            if (failRemember != null) throw failRemember;
            remembered.add(Map.entry(key, jobId));
            if (store) entries.put(key, jobId);
        }
    }

    /** Wraps a repository and counts {@link JobRepository#insert} calls (the database path of a submission). */
    private static JobRepository countingInserts(JobRepository delegate, AtomicInteger inserts) {
        return (JobRepository) Proxy.newProxyInstance(JobRepository.class.getClassLoader(),
                new Class<?>[] {JobRepository.class}, (proxy, method, args) -> {
                    if (method.getName().equals("insert")) inserts.incrementAndGet();
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }
}
