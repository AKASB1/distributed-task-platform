package io.akasb.taskplatform.worker.handlers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.akasb.taskplatform.worker.NonRetryableTaskException;
import io.akasb.taskplatform.worker.RetryableTaskException;
import io.akasb.taskplatform.worker.TaskContext;
import io.akasb.taskplatform.worker.TaskHandler;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntPredicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The two sample handlers against a small fake {@link TaskContext}: durations and progress steps, cancellation and
 * interruption, the flaky handler's failure modes, and payload defaults. Handler threads are coordinated with
 * latches; nothing here sleeps on the test side.
 */
class SleepAndFlakyHandlerTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final SleepTaskHandler sleep = new SleepTaskHandler();
    private final FlakyTaskHandler flaky = new FlakyTaskHandler();
    private Thread worker;

    @AfterEach
    void tearDown() throws InterruptedException {
        Thread.interrupted(); // never leak an interrupt flag set by a test into the next one
        if (worker != null && worker.isAlive()) {
            worker.interrupt();
            worker.join(TimeUnit.SECONDS.toMillis(5));
        }
    }

    /** Records progress, can be cancelled from the outside or from a progress callback. */
    private static final class FakeContext implements TaskContext {
        private final UUID jobId = new UUID(0xFA4E, 1);
        private final int attempt;
        private final JsonNode payload;
        final List<Integer> progress = new CopyOnWriteArrayList<>();
        final AtomicBoolean cancelled = new AtomicBoolean();
        /** Counts down the first time the handler checks for cancellation (it is running). */
        final CountDownLatch checked = new CountDownLatch(1);
        /** Cancels the attempt when a reported percentage matches. */
        volatile IntPredicate cancelWhenProgress = p -> false;

        FakeContext(int attempt, JsonNode payload) {
            this.attempt = attempt;
            this.payload = payload;
        }

        @Override public UUID jobId() { return jobId; }
        @Override public int attempt() { return attempt; }
        @Override public String queue() { return "default"; }
        @Override public JsonNode payload() { return payload; }

        @Override
        public void reportProgress(int percent) {
            progress.add(percent);
            if (cancelWhenProgress.test(percent)) cancelled.set(true);
        }

        @Override
        public boolean isCancelled() {
            checked.countDown();
            return cancelled.get();
        }
    }

    private static ObjectNode payload() {
        return MAPPER.createObjectNode();
    }

    private static FakeContext ctx(int attempt, ObjectNode payload) {
        return new FakeContext(attempt, payload);
    }

    /** Runs {@code handler} on a dedicated platform thread; the outcome lands in {@code outcome}. */
    private Thread runAsync(TaskHandler handler, FakeContext context, AtomicReference<Object> outcome) {
        worker = Thread.ofPlatform().name("handler-under-test").daemon(true).start(() -> {
            try {
                outcome.set(handler.handle(context));
            } catch (Throwable t) {
                outcome.set(t);
            }
        });
        return worker;
    }

    private static void awaitEnd(Thread thread) throws InterruptedException {
        thread.join(TimeUnit.SECONDS.toMillis(10));
        assertThat(thread.isAlive()).as("handler stopped").isFalse();
    }

    // ----------------------------------------------------------------------------------- sleep

    @Test
    void sleepTypeIsSleep() {
        assertThat(sleep.type()).isEqualTo("sleep").isEqualTo(SleepTaskHandler.TYPE);
        assertThat(SleepTaskHandler.MAX_DURATION_MS).isEqualTo(3_600_000L);
    }

    @Test
    void sleepWorksForTheRequestedDurationInTenSteps() throws Exception {
        FakeContext c = ctx(2, payload().put("durationMs", 120));
        long start = System.nanoTime();
        Object result = sleep.handle(c);
        long elapsed = System.nanoTime() - start;

        assertThat(result).isEqualTo(Map.of("sleptMs", 120L, "attempt", 2));
        assertThat(c.progress).containsExactly(10, 20, 30, 40, 50, 60, 70, 80, 90, 100);
        assertThat(elapsed).isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(120));
    }

    @Test
    void shortDurationsUseFewerStepsOfAtLeastTenMilliseconds() throws Exception {
        FakeContext thirty = ctx(1, payload().put("durationMs", 30));
        sleep.handle(thirty);
        assertThat(thirty.progress).containsExactly(33, 66, 100);

        FakeContext twentyFive = ctx(1, payload().put("durationMs", 25));
        long start = System.nanoTime();
        sleep.handle(twentyFive);
        assertThat(twentyFive.progress).as("2 steps of 12 ms + 1 ms remainder").containsExactly(50, 100);
        assertThat(System.nanoTime() - start).isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(25));

        FakeContext nine = ctx(1, payload().put("durationMs", 9));
        sleep.handle(nine);
        assertThat(nine.progress).containsExactly(100);
    }

    @Test
    void zeroOrNegativeDurationCompletesAtOnceWithFullProgress() throws Exception {
        FakeContext zero = ctx(1, payload().put("durationMs", 0));
        assertThat(sleep.handle(zero)).isEqualTo(Map.of("sleptMs", 0L, "attempt", 1));
        assertThat(zero.progress).containsExactly(100);

        FakeContext negative = ctx(3, payload().put("durationMs", -500));
        assertThat(sleep.handle(negative)).isEqualTo(Map.of("sleptMs", 0L, "attempt", 3));
        assertThat(negative.progress).containsExactly(100);
    }

    @Test
    void sleepDefaultsToOneHundredMilliseconds() throws Exception {
        FakeContext c = ctx(1, payload());
        long start = System.nanoTime();
        assertThat(sleep.handle(c)).isEqualTo(Map.of("sleptMs", 100L, "attempt", 1));
        assertThat(c.progress).hasSize(10).endsWith(100);
        assertThat(System.nanoTime() - start).isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(100));
    }

    @Test
    void sleepAcceptsANumericString() throws Exception {
        assertThat(sleep.handle(ctx(1, payload().put("durationMs", "20")))).isEqualTo(Map.of("sleptMs", 20L,
                "attempt", 1));
    }

    @Test
    void sleepStopsBeforeWorkingWhenAlreadyCancelled() {
        FakeContext c = ctx(1, payload().put("durationMs", 60_000));
        c.cancelled.set(true);
        assertThatThrownBy(() -> sleep.handle(c)).isInstanceOf(InterruptedException.class);
        assertThat(c.progress).isEmpty();
    }

    @Test
    void sleepStopsAtTheNextStepAfterCancellation() {
        FakeContext c = ctx(1, payload().put("durationMs", 100));
        c.cancelWhenProgress = p -> p >= 30;
        assertThatThrownBy(() -> sleep.handle(c)).isInstanceOf(InterruptedException.class);
        assertThat(c.progress).containsExactly(10, 20, 30);
    }

    @Test
    void sleepHonoursTheInterruptFlagOfItsThread() {
        FakeContext c = ctx(1, payload().put("durationMs", 60_000));
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> sleep.handle(c)).isInstanceOf(InterruptedException.class)
                    .hasMessage("attempt stopped");
        } finally {
            Thread.interrupted();
        }
        assertThat(c.progress).isEmpty();
    }

    @Test
    @Timeout(20)
    void aLongSleepStopsPromptlyWhenItsThreadIsInterrupted() throws Exception {
        FakeContext c = ctx(1, payload().put("durationMs", 3_000_000));
        AtomicReference<Object> outcome = new AtomicReference<>();
        Thread t = runAsync(sleep, c, outcome);
        assertThat(c.checked.await(5, TimeUnit.SECONDS)).isTrue();

        long start = System.nanoTime();
        t.interrupt();
        awaitEnd(t);

        assertThat(outcome.get()).isInstanceOf(InterruptedException.class);
        assertThat(System.nanoTime() - start).isLessThan(TimeUnit.SECONDS.toNanos(5));
        assertThat(c.progress).isEmpty();
    }

    // ----------------------------------------------------------------------------------- flaky

    @Test
    void flakyTypeIsFlaky() {
        assertThat(flaky.type()).isEqualTo("flaky").isEqualTo(FlakyTaskHandler.TYPE);
    }

    @Test
    void flakyDefaultsFailTheFirstAttemptRetryablyAfterFiftyMilliseconds() throws Exception {
        FakeContext first = ctx(1, payload());
        long start = System.nanoTime();
        assertThatThrownBy(() -> flaky.handle(first))
                .isExactlyInstanceOf(RetryableTaskException.class)
                .hasMessage("injected failure on attempt 1 of 1");
        assertThat(System.nanoTime() - start).isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(50));
        assertThat(first.progress).as("50 ms in 5 steps").containsExactly(20, 40, 60, 80, 100);

        FakeContext second = ctx(2, payload());
        assertThat(flaky.handle(second)).isEqualTo(Map.of("succeededOnAttempt", 2));
        assertThat(second.progress).containsExactly(20, 40, 60, 80, 100);
    }

    @Test
    void flakyFailsExactlyTheFirstFailAttemptsAttempts() throws Exception {
        ObjectNode p = payload().put("durationMs", 0).put("failAttempts", 3).put("failure", "retryable");
        for (int attempt = 1; attempt <= 3; attempt++) {
            FakeContext c = ctx(attempt, p);
            assertThatThrownBy(() -> flaky.handle(c))
                    .isExactlyInstanceOf(RetryableTaskException.class)
                    .hasMessage("injected failure on attempt " + attempt + " of 3");
        }
        assertThat(flaky.handle(ctx(4, p))).isEqualTo(Map.of("succeededOnAttempt", 4));
        assertThat(flaky.handle(ctx(9, p))).isEqualTo(Map.of("succeededOnAttempt", 9));
    }

    @Test
    void flakyWithZeroFailAttemptsSucceedsImmediately() throws Exception {
        ObjectNode p = payload().put("durationMs", 0).put("failAttempts", 0);
        assertThat(flaky.handle(ctx(1, p))).isEqualTo(Map.of("succeededOnAttempt", 1));
    }

    @Test
    void flakyPermanentFailureIsNonRetryable() throws Exception {
        ObjectNode p = payload().put("durationMs", 10).put("failAttempts", 2).put("failure", "permanent");
        FakeContext c = ctx(2, p);
        assertThatThrownBy(() -> flaky.handle(c))
                .isExactlyInstanceOf(NonRetryableTaskException.class)
                .hasMessage("injected failure on attempt 2 of 2");
        assertThat(c.progress).as("work happens before the failure").containsExactly(100);
        assertThat(flaky.handle(ctx(3, p))).isEqualTo(Map.of("succeededOnAttempt", 3));
    }

    @Test
    void flakyUnknownFailureModeFallsBackToRetryable() {
        ObjectNode p = payload().put("durationMs", 0).put("failure", "sometimes");
        assertThatThrownBy(() -> flaky.handle(ctx(1, p))).isExactlyInstanceOf(RetryableTaskException.class);
    }

    @Test
    void flakyNegativeDurationIsTreatedAsZero() throws Exception {
        FakeContext c = ctx(2, payload().put("durationMs", -1).put("failAttempts", 1));
        assertThat(flaky.handle(c)).isEqualTo(Map.of("succeededOnAttempt", 2));
        assertThat(c.progress).containsExactly(100);
    }

    @Test
    void flakyStopsAtTheNextStepAfterCancellation() {
        FakeContext c = ctx(2, payload().put("durationMs", 100).put("failAttempts", 1));
        c.cancelWhenProgress = p -> p >= 50;
        assertThatThrownBy(() -> flaky.handle(c)).isInstanceOf(InterruptedException.class);
        assertThat(c.progress).containsExactly(10, 20, 30, 40, 50);
    }

    @Test
    void flakyHangThatIsAlreadyCancelledStopsAtOnce() {
        FakeContext c = ctx(1, payload().put("failure", "hang"));
        c.cancelled.set(true);
        assertThatThrownBy(() -> flaky.handle(c)).isInstanceOf(InterruptedException.class);
        assertThat(c.progress).isEmpty();
    }

    @Test
    @Timeout(20)
    void flakyHangBlocksUntilCancelled() throws Exception {
        FakeContext c = ctx(1, payload().put("failure", "hang").put("failAttempts", 1));
        AtomicReference<Object> outcome = new AtomicReference<>();
        Thread t = runAsync(flaky, c, outcome);
        assertThat(c.checked.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(t.isAlive()).as("still hanging").isTrue();

        c.cancelled.set(true);
        awaitEnd(t);

        assertThat(outcome.get()).isInstanceOf(InterruptedException.class);
        assertThat(c.progress).as("a hanging attempt reports no progress").isEmpty();
    }

    @Test
    @Timeout(20)
    void flakyHangStopsPromptlyWhenItsThreadIsInterrupted() throws Exception {
        FakeContext c = ctx(1, payload().put("failure", "hang"));
        AtomicReference<Object> outcome = new AtomicReference<>();
        Thread t = runAsync(flaky, c, outcome);
        assertThat(c.checked.await(5, TimeUnit.SECONDS)).isTrue();

        long start = System.nanoTime();
        t.interrupt();
        awaitEnd(t);

        assertThat(outcome.get()).isInstanceOf(InterruptedException.class);
        assertThat(System.nanoTime() - start).isLessThan(TimeUnit.SECONDS.toNanos(5));
    }

    @Test
    void flakyHangOnlyAffectsTheFailingAttempts() throws Exception {
        ObjectNode p = payload().put("durationMs", 0).put("failure", "hang").put("failAttempts", 1);
        assertThat(flaky.handle(ctx(2, p))).isEqualTo(Map.of("succeededOnAttempt", 2));
    }

    @Test
    void flakyFailProbabilityIsDeterministicPerSeedAndAttempt() throws Exception {
        ObjectNode p = payload().put("durationMs", 0).put("failAttempts", 1).put("failProbability", 0.3)
                .put("failSeed", 1234L);
        for (int attempt = 2; attempt <= 6; attempt++) {
            boolean first = fails(p, attempt);
            assertThat(fails(p, attempt)).as("attempt %d decided the same way twice", attempt).isEqualTo(first);
        }
        assertThatThrownBy(() -> flaky.handle(ctx(1, p))).isExactlyInstanceOf(RetryableTaskException.class)
                .hasMessageContaining("attempt 1 of 1");
    }

    @Test
    void flakyFailProbabilityBounds() throws Exception {
        ObjectNode never = payload().put("durationMs", 0).put("failAttempts", 0).put("failProbability", 0.0);
        ObjectNode always = payload().put("durationMs", 0).put("failAttempts", 0).put("failProbability", 1.0);
        for (int attempt = 1; attempt <= 5; attempt++) {
            assertThat(fails(never, attempt)).isFalse();
            assertThat(fails(always, attempt)).isTrue();
        }
        int failures = 0;
        for (long seed = 0; seed < 5_000; seed++) {
            if (fails(payload().put("durationMs", 0).put("failAttempts", 0).put("failProbability", 0.3)
                    .put("failSeed", seed), 2)) failures++;
        }
        assertThat(failures / 5_000.0).isBetween(0.27, 0.33);
    }

    private boolean fails(ObjectNode payload, int attempt) throws Exception {
        try {
            flaky.handle(ctx(attempt, payload));
            return false;
        } catch (RetryableTaskException e) {
            return true;
        }
    }
}
