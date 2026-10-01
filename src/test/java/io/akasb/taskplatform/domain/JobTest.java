package io.akasb.taskplatform.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The immutable job snapshot: every factory and transition method, what each one carries over or resets, the
 * version/attempt bookkeeping, illegal transitions for all 8 states × 7 methods, and constructor validation.
 */
class JobTest {
    private static final UUID ID = new UUID(1, 2);
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final Instant T1 = T0.plusSeconds(1);
    private static final Instant T2 = T0.plusSeconds(2);
    private static final Instant T3 = T0.plusSeconds(3);
    private static final Instant T4 = T0.plusSeconds(4);
    private static final Instant T5 = T0.plusSeconds(5);
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private static Job pending() {
        return Job.pending(ID, "default", "sleep", "{\"durationMs\":5}", 3, TIMEOUT, "key-1", T0);
    }

    // ------------------------------------------------------------------------------------ factory

    @Test
    void pendingIsVersionZeroWithNoAttemptsAndNoTimestampsBeyondCreation() {
        Job job = pending();

        assertThat(job.id()).isEqualTo(ID);
        assertThat(job.queue()).isEqualTo("default");
        assertThat(job.type()).isEqualTo("sleep");
        assertThat(job.payload()).isEqualTo("{\"durationMs\":5}");
        assertThat(job.state()).isEqualTo(JobState.PENDING);
        assertThat(job.version()).isZero();
        assertThat(job.attempts()).isZero();
        assertThat(job.maxAttempts()).isEqualTo(3);
        assertThat(job.timeout()).isEqualTo(TIMEOUT);
        assertThat(job.idempotencyKey()).isEqualTo("key-1");
        assertThat(job.createdAt()).isEqualTo(T0);
        assertThat(job.updatedAt()).isEqualTo(T0);
        assertThat(job.readyAt()).isNull();
        assertThat(job.dispatchedAt()).isNull();
        assertThat(job.nextAttemptAt()).isNull();
        assertThat(job.finishedAt()).isNull();
        assertThat(job.lastError()).isNull();
        assertThat(job.result()).isNull();
        assertThat(job.hasAttemptsLeft()).isTrue();
    }

    @Test
    void pendingAcceptsANullIdempotencyKey() {
        Job job = Job.pending(ID, "q", "t", "{}", 1, TIMEOUT, null, T0);
        assertThat(job.idempotencyKey()).isNull();
    }

    // -------------------------------------------------------------------------------- transitions

    @Test
    void enqueueFromPendingSetsReadyAtAndDispatchedAtAndBumpsVersionOnly() {
        Job pending = pending();
        Job queued = pending.enqueue(T0, T1);

        assertThat(queued.state()).isEqualTo(JobState.QUEUED);
        assertThat(queued.version()).isEqualTo(1);
        assertThat(queued.attempts()).isZero();
        assertThat(queued.readyAt()).isEqualTo(T0);
        assertThat(queued.dispatchedAt()).isEqualTo(T1);
        assertThat(queued.updatedAt()).isEqualTo(T1);
        assertThat(queued.nextAttemptAt()).isNull();
        assertThat(queued.finishedAt()).isNull();
        assertThat(queued.lastError()).isNull();
        assertThat(queued.result()).isNull();
        assertUnchangedIdentity(pending, queued);
    }

    @Test
    void startIsTheOnlyTransitionThatIncrementsAttempts() {
        Job queued = pending().enqueue(T0, T1);
        Job running = queued.start(T2);

        assertThat(running.state()).isEqualTo(JobState.RUNNING);
        assertThat(running.version()).isEqualTo(2);
        assertThat(running.attempts()).isEqualTo(1);
        assertThat(running.readyAt()).as("queue wait start is kept for latency").isEqualTo(T0);
        assertThat(running.dispatchedAt()).isEqualTo(T1);
        assertThat(running.updatedAt()).isEqualTo(T2);
        assertThat(running.nextAttemptAt()).isNull();
        assertThat(running.finishedAt()).isNull();
        assertUnchangedIdentity(queued, running);
    }

    @Test
    void succeedStoresResultSetsFinishedAtAndClearsLastError() {
        Job running = afterOneRetry().start(T4);
        assertThat(running.lastError()).isEqualTo("RETRYABLE: boom");

        Job done = running.succeed("{\"ok\":true}", T5);

        assertThat(done.state()).isEqualTo(JobState.SUCCEEDED);
        assertThat(done.version()).isEqualTo(running.version() + 1);
        assertThat(done.attempts()).isEqualTo(running.attempts());
        assertThat(done.result()).isEqualTo("{\"ok\":true}");
        assertThat(done.finishedAt()).isEqualTo(T5);
        assertThat(done.updatedAt()).isEqualTo(T5);
        assertThat(done.lastError()).isNull();
        assertThat(done.nextAttemptAt()).isNull();
        assertThat(done.readyAt()).isEqualTo(running.readyAt());
        assertThat(done.dispatchedAt()).isEqualTo(running.dispatchedAt());
        assertUnchangedIdentity(running, done);
    }

    @Test
    void succeedAcceptsANullResult() {
        Job done = pending().enqueue(T0, T0).start(T1).succeed(null, T2);
        assertThat(done.state()).isEqualTo(JobState.SUCCEEDED);
        assertThat(done.result()).isNull();
        assertThat(done.finishedAt()).isEqualTo(T2);
    }

    @Test
    void retryAtRecordsNextAttemptAndErrorWithoutFinishing() {
        Job running = pending().enqueue(T0, T0).start(T1);
        Job waiting = running.retryAt(T3, "RETRYABLE: boom", T2);

        assertThat(waiting.state()).isEqualTo(JobState.RETRY_WAIT);
        assertThat(waiting.version()).isEqualTo(running.version() + 1);
        assertThat(waiting.attempts()).isEqualTo(1);
        assertThat(waiting.nextAttemptAt()).isEqualTo(T3);
        assertThat(waiting.lastError()).isEqualTo("RETRYABLE: boom");
        assertThat(waiting.finishedAt()).isNull();
        assertThat(waiting.updatedAt()).isEqualTo(T2);
        assertThat(waiting.readyAt()).isEqualTo(T0);
        assertThat(waiting.dispatchedAt()).isEqualTo(T0);
        assertThat(waiting.result()).isNull();
        assertUnchangedIdentity(running, waiting);
    }

    @Test
    void reEnqueueAfterRetryClearsNextAttemptKeepsLastErrorAndStartsANewQueueWait() {
        Job waiting = pending().enqueue(T0, T0).start(T1).retryAt(T3, "RETRYABLE: boom", T2);
        Job requeued = waiting.enqueue(waiting.nextAttemptAt(), T4);

        assertThat(requeued.state()).isEqualTo(JobState.QUEUED);
        assertThat(requeued.version()).isEqualTo(waiting.version() + 1);
        assertThat(requeued.attempts()).isEqualTo(1);
        assertThat(requeued.readyAt()).isEqualTo(T3);
        assertThat(requeued.dispatchedAt()).isEqualTo(T4);
        assertThat(requeued.nextAttemptAt()).isNull();
        assertThat(requeued.finishedAt()).isNull();
        assertThat(requeued.lastError()).as("the previous attempt's error stays visible").isEqualTo("RETRYABLE: boom");

        Job second = requeued.start(T5);
        assertThat(second.attempts()).isEqualTo(2);
        assertThat(second.lastError()).isEqualTo("RETRYABLE: boom");
        assertThat(second.readyAt()).isEqualTo(T3);
    }

    @Test
    void failSetsFinishedAtAndError() {
        Job running = pending().enqueue(T0, T0).start(T1);
        Job failed = running.fail("NON_RETRYABLE: bad input", T2);

        assertThat(failed.state()).isEqualTo(JobState.FAILED);
        assertThat(failed.version()).isEqualTo(running.version() + 1);
        assertThat(failed.attempts()).isEqualTo(1);
        assertThat(failed.finishedAt()).isEqualTo(T2);
        assertThat(failed.updatedAt()).isEqualTo(T2);
        assertThat(failed.lastError()).isEqualTo("NON_RETRYABLE: bad input");
        assertThat(failed.nextAttemptAt()).isNull();
        assertThat(failed.result()).isNull();
        assertUnchangedIdentity(running, failed);
    }

    @Test
    void deadLetterSetsFinishedAtAndError() {
        Job running = afterOneRetry().start(T4);
        Job dead = running.deadLetter("attempts exhausted (2/3)", T5);

        assertThat(dead.state()).isEqualTo(JobState.DEAD_LETTER);
        assertThat(dead.version()).isEqualTo(running.version() + 1);
        assertThat(dead.attempts()).isEqualTo(2);
        assertThat(dead.finishedAt()).isEqualTo(T5);
        assertThat(dead.updatedAt()).isEqualTo(T5);
        assertThat(dead.lastError()).isEqualTo("attempts exhausted (2/3)");
        assertThat(dead.nextAttemptAt()).isNull();
        assertUnchangedIdentity(running, dead);
    }

    @Test
    void cancelFromEveryCancellableStateFinishesAndKeepsLastError() {
        Job fromPending = pending().cancel(T1);
        assertThat(fromPending.state()).isEqualTo(JobState.CANCELLED);
        assertThat(fromPending.version()).isEqualTo(1);
        assertThat(fromPending.attempts()).isZero();
        assertThat(fromPending.finishedAt()).isEqualTo(T1);
        assertThat(fromPending.updatedAt()).isEqualTo(T1);
        assertThat(fromPending.readyAt()).isNull();

        Job queued = pending().enqueue(T0, T0);
        Job fromQueued = queued.cancel(T1);
        assertThat(fromQueued.version()).isEqualTo(2);
        assertThat(fromQueued.attempts()).isZero();
        assertThat(fromQueued.readyAt()).isEqualTo(T0);
        assertThat(fromQueued.finishedAt()).isEqualTo(T1);

        Job running = queued.start(T1);
        Job fromRunning = running.cancel(T2);
        assertThat(fromRunning.version()).isEqualTo(running.version() + 1);
        assertThat(fromRunning.attempts()).isEqualTo(1);
        assertThat(fromRunning.finishedAt()).isEqualTo(T2);

        Job waiting = retryWait();
        assertThat(waiting.nextAttemptAt()).isEqualTo(T3);
        Job fromRetryWait = waiting.cancel(T4);
        assertThat(fromRetryWait.state()).isEqualTo(JobState.CANCELLED);
        assertThat(fromRetryWait.version()).isEqualTo(waiting.version() + 1);
        assertThat(fromRetryWait.attempts()).isEqualTo(1);
        assertThat(fromRetryWait.finishedAt()).isEqualTo(T4);
        assertThat(fromRetryWait.nextAttemptAt()).as("no further attempt is scheduled").isNull();
        assertThat(fromRetryWait.lastError()).isEqualTo("RETRYABLE: boom");
    }

    @Test
    void fullRetryPathBumpsVersionOnEveryStepAndAttemptsOnlyOnStart() {
        List<Job> path = new ArrayList<>();
        path.add(pending());
        path.add(path.getLast().enqueue(T0, T0));
        path.add(path.getLast().start(T1));
        path.add(path.getLast().retryAt(T3, "RETRYABLE: boom", T2));
        path.add(path.getLast().enqueue(T3, T3));
        path.add(path.getLast().start(T4));
        path.add(path.getLast().succeed("{}", T5));

        assertThat(path).extracting(Job::version).containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L);
        assertThat(path).extracting(Job::attempts).containsExactly(0, 0, 1, 1, 1, 2, 2);
        assertThat(path).extracting(Job::state).containsExactly(JobState.PENDING, JobState.QUEUED,
                JobState.RUNNING, JobState.RETRY_WAIT, JobState.QUEUED, JobState.RUNNING, JobState.SUCCEEDED);
        assertThat(path).extracting(Job::createdAt).containsOnly(T0);
    }

    @Test
    void transitionsLeaveTheOriginalSnapshotUntouched() {
        Job pending = pending();
        Job copy = new Job(pending.id(), pending.queue(), pending.type(), pending.payload(), pending.state(),
                pending.version(), pending.attempts(), pending.maxAttempts(), pending.timeout(),
                pending.idempotencyKey(), pending.createdAt(), pending.updatedAt(), pending.readyAt(),
                pending.dispatchedAt(), pending.nextAttemptAt(), pending.finishedAt(), pending.lastError(),
                pending.result());

        pending.enqueue(T1, T1).start(T2).succeed("{}", T3);
        pending.cancel(T1);

        assertThat(pending).isEqualTo(copy);
    }

    @Test
    void hasAttemptsLeftComparesAttemptsWithMaxAttempts() {
        Job queued = Job.pending(ID, "q", "t", "{}", 2, TIMEOUT, null, T0).enqueue(T0, T0);
        Job first = queued.start(T1);
        assertThat(first.hasAttemptsLeft()).isTrue();

        Job second = first.retryAt(T2, "e", T1).enqueue(T2, T2).start(T3);
        assertThat(second.attempts()).isEqualTo(2);
        assertThat(second.hasAttemptsLeft()).isFalse();
    }

    // ------------------------------------------------------------------- legal and illegal moves

    /** One transition method of {@link Job}, named by the state it targets. */
    private record Move(JobState target, Function<Job, Job> apply) {
        @Override
        public String toString() {
            return "->" + target;
        }
    }

    private static final List<Move> MOVES = List.of(
            new Move(JobState.QUEUED, j -> j.enqueue(T4, T5)),
            new Move(JobState.RUNNING, j -> j.start(T5)),
            new Move(JobState.SUCCEEDED, j -> j.succeed("{}", T5)),
            new Move(JobState.RETRY_WAIT, j -> j.retryAt(T5.plusSeconds(10), "err", T5)),
            new Move(JobState.FAILED, j -> j.fail("err", T5)),
            new Move(JobState.DEAD_LETTER, j -> j.deadLetter("err", T5)),
            new Move(JobState.CANCELLED, j -> j.cancel(T5)));

    /** A job in {@code state}, reached only through legal transitions. */
    private static Job inState(JobState state) {
        Job running = pending().enqueue(T0, T0).start(T1);
        return switch (state) {
            case PENDING -> pending();
            case QUEUED -> pending().enqueue(T0, T0);
            case RUNNING -> running;
            case RETRY_WAIT -> retryWait();
            case SUCCEEDED -> running.succeed("{}", T2);
            case FAILED -> running.fail("err", T2);
            case DEAD_LETTER -> running.deadLetter("err", T2);
            case CANCELLED -> running.cancel(T2);
        };
    }

    static Stream<Arguments> everyStateAndMove() {
        List<Arguments> cases = new ArrayList<>();
        for (JobState from : JobState.values()) {
            for (Move move : MOVES) cases.add(Arguments.of(from, move));
        }
        return cases.stream();
    }

    @Test
    void movesCoverEveryReachableTarget() {
        assertThat(MOVES).extracting(Move::target)
                .containsExactlyInAnyOrder(JobState.QUEUED, JobState.RUNNING, JobState.SUCCEEDED,
                        JobState.RETRY_WAIT, JobState.FAILED, JobState.DEAD_LETTER, JobState.CANCELLED);
        assertThat(everyStateAndMove()).hasSize(56);
        for (JobState s : JobState.values()) assertThat(inState(s).state()).isEqualTo(s);
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("everyStateAndMove")
    void everyMethodFollowsTheTransitionTable(JobState from, Move move) {
        Job before = inState(from);
        if (from.canTransitionTo(move.target())) {
            Job after = move.apply().apply(before);
            assertThat(after.state()).isEqualTo(move.target());
            assertThat(after.version()).isEqualTo(before.version() + 1);
            assertThat(after.attempts())
                    .isEqualTo(before.attempts() + (move.target() == JobState.RUNNING ? 1 : 0));
            assertThat(after.updatedAt()).isEqualTo(T5);
            assertThat(after.finishedAt()).isEqualTo(move.target().isTerminal() ? T5 : null);
            assertUnchangedIdentity(before, after);
        } else {
            assertThatThrownBy(() -> move.apply().apply(before))
                    .isInstanceOf(IllegalTransitionException.class)
                    .hasMessage("invalid transition " + from + " -> " + move.target())
                    .satisfies(e -> {
                        IllegalTransitionException ite = (IllegalTransitionException) e;
                        assertThat(ite.from()).isEqualTo(from);
                        assertThat(ite.to()).isEqualTo(move.target());
                    });
        }
    }

    @Test
    void illegalTransitionExceptionIsAnIllegalStateException() {
        assertThatThrownBy(() -> pending().start(T1)).isInstanceOf(IllegalStateException.class);
    }

    // ------------------------------------------------------------------------- constructor checks

    /** All 18 components of a valid job; tests change one and call {@link #build()}. */
    private static final class Fields {
        UUID id = ID;
        String queue = "q";
        String type = "t";
        String payload = "{}";
        JobState state = JobState.PENDING;
        long version = 0;
        int attempts = 0;
        int maxAttempts = 1;
        Duration timeout = TIMEOUT;
        String idempotencyKey = null;
        Instant createdAt = T0;
        Instant updatedAt = T0;
        Instant readyAt = null;
        Instant dispatchedAt = null;
        Instant nextAttemptAt = null;
        Instant finishedAt = null;
        String lastError = null;
        String result = null;

        Job build() {
            return new Job(id, queue, type, payload, state, version, attempts, maxAttempts, timeout, idempotencyKey,
                    createdAt, updatedAt, readyAt, dispatchedAt, nextAttemptAt, finishedAt, lastError, result);
        }
    }

    private static Job build(Consumer<Fields> change) {
        Fields f = new Fields();
        change.accept(f);
        return f.build();
    }

    static Stream<Arguments> requiredComponents() {
        return Stream.of(
                Arguments.of("id", (Consumer<Fields>) f -> f.id = null),
                Arguments.of("queue", (Consumer<Fields>) f -> f.queue = null),
                Arguments.of("type", (Consumer<Fields>) f -> f.type = null),
                Arguments.of("payload", (Consumer<Fields>) f -> f.payload = null),
                Arguments.of("state", (Consumer<Fields>) f -> f.state = null),
                Arguments.of("timeout", (Consumer<Fields>) f -> f.timeout = null),
                Arguments.of("createdAt", (Consumer<Fields>) f -> f.createdAt = null),
                Arguments.of("updatedAt", (Consumer<Fields>) f -> f.updatedAt = null));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("requiredComponents")
    void rejectsMissingRequiredComponent(String name, Consumer<Fields> change) {
        assertThatThrownBy(() -> build(change)).isInstanceOf(NullPointerException.class).hasMessage(name);
    }

    static Stream<Arguments> invalidValues() {
        return Stream.of(
                Arguments.of("blank queue", (Consumer<Fields>) f -> f.queue = "  ", "queue required"),
                Arguments.of("empty queue", (Consumer<Fields>) f -> f.queue = "", "queue required"),
                Arguments.of("blank type", (Consumer<Fields>) f -> f.type = "\t", "type required"),
                Arguments.of("maxAttempts 0", (Consumer<Fields>) f -> f.maxAttempts = 0, "maxAttempts must be >= 1"),
                Arguments.of("maxAttempts -1", (Consumer<Fields>) f -> f.maxAttempts = -1,
                        "maxAttempts must be >= 1"),
                Arguments.of("attempts -1", (Consumer<Fields>) f -> f.attempts = -1, "attempts must be >= 0"),
                Arguments.of("zero timeout", (Consumer<Fields>) f -> f.timeout = Duration.ZERO,
                        "timeout must be positive"),
                Arguments.of("negative timeout", (Consumer<Fields>) f -> f.timeout = Duration.ofMillis(-1),
                        "timeout must be positive"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidValues")
    void rejectsInvalidValue(String name, Consumer<Fields> change, String message) {
        assertThatThrownBy(() -> build(change)).isInstanceOf(IllegalArgumentException.class).hasMessage(message);
    }

    @Test
    void optionalComponentsMayBeNullAndBoundaryValuesAreAccepted() {
        assertThatNoException().isThrownBy(new Fields()::build);
        Job minimal = build(f -> {
            f.maxAttempts = 1;
            f.attempts = 0;
            f.timeout = Duration.ofNanos(1);
        });
        assertThat(minimal.timeout()).isEqualTo(Duration.ofNanos(1));
        assertThat(minimal.idempotencyKey()).isNull();
        assertThat(minimal.readyAt()).isNull();
        assertThat(minimal.lastError()).isNull();
    }

    // -------------------------------------------------------------------------------- helpers

    /** Attempt 1 failed retryably at T2; RETRY_WAIT until T3, version 3. */
    private static Job retryWait() {
        return pending().enqueue(T0, T0).start(T1).retryAt(T3, "RETRYABLE: boom", T2);
    }

    /** {@link #retryWait()} released at T3: QUEUED again with attempts 1, version 4. */
    private static Job afterOneRetry() {
        return retryWait().enqueue(T3, T3);
    }

    private static void assertUnchangedIdentity(Job before, Job after) {
        assertThat(after.id()).isEqualTo(before.id());
        assertThat(after.queue()).isEqualTo(before.queue());
        assertThat(after.type()).isEqualTo(before.type());
        assertThat(after.payload()).isEqualTo(before.payload());
        assertThat(after.maxAttempts()).isEqualTo(before.maxAttempts());
        assertThat(after.timeout()).isEqualTo(before.timeout());
        assertThat(after.idempotencyKey()).isEqualTo(before.idempotencyKey());
        assertThat(after.createdAt()).isEqualTo(before.createdAt());
    }
}
