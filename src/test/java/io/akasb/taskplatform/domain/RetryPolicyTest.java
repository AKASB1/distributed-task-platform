package io.akasb.taskplatform.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.akasb.taskplatform.domain.RetryPolicy.RetryDecision;
import io.akasb.taskplatform.domain.RetryPolicy.RetryDecision.Action;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.random.RandomGenerator;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Capped exponential backoff with bounded jitter. Bounds are checked against the exact decimal value of
 * {@code (1 − jitter) · base} (BigDecimal), so floating-point noise in the test cannot hide or invent a violation.
 * Every random source is seeded.
 */
class RetryPolicyTest {
    private static final int SAMPLES = 10_000;

    /** Policy parameters in milliseconds. */
    private record Config(long initialMs, double multiplier, long maxMs, double jitter) {
        RetryPolicy policy(RandomGenerator random) {
            return new RetryPolicy(Duration.ofMillis(initialMs), multiplier, Duration.ofMillis(maxMs), jitter, random);
        }

        /** The first attempt whose base delay is the cap, plus two more. */
        int attemptsPastCap() {
            int n = 1;
            while (n < 200 && initialMs * Math.pow(multiplier, n - 1) < maxMs) n++;
            return n + 2;
        }

        /** The last attempt whose base delay is still strictly below the cap. */
        int lastAttemptBelowCap() {
            int n = 1;
            while (initialMs * Math.pow(multiplier, n) < maxMs) n++;
            return n;
        }

        @Override
        public String toString() {
            return "initial=" + initialMs + "ms x" + multiplier + " cap=" + maxMs + "ms jitter=" + jitter;
        }
    }

    private static RetryPolicy policy(long initialMs, double multiplier, long maxMs, double jitter, long seed) {
        return new Config(initialMs, multiplier, maxMs, jitter).policy(new Random(seed));
    }

    /** Exact {@code (1 − jitter) · base} in milliseconds, using the decimal value the jitter was written as. */
    private static BigDecimal lowerBound(long baseMs, double jitter) {
        return BigDecimal.ONE.subtract(BigDecimal.valueOf(jitter)).multiply(BigDecimal.valueOf(baseMs));
    }

    // ------------------------------------------------------------------------------ baseDelay

    @Test
    void baseDelayGrowsByTheMultiplierUntilTheCap() {
        RetryPolicy p = policy(100, 2.0, 10_000, 0.5, 1);
        long[] expected = {100, 200, 400, 800, 1_600, 3_200, 6_400, 10_000, 10_000, 10_000};
        for (int n = 1; n <= expected.length; n++) {
            assertThat(p.baseDelay(n)).as("attempt " + n).isEqualTo(Duration.ofMillis(expected[n - 1]));
        }
        for (int n = 1; n < 7; n++) {
            assertThat(p.baseDelay(n + 1).toMillis()).isEqualTo(p.baseDelay(n).toMillis() * 2);
        }
    }

    @Test
    void baseDelayStaysAtTheCapForHugeAttemptNumbers() {
        RetryPolicy p = policy(100, 2.0, 10_000, 0.5, 1);
        assertThat(p.baseDelay(64)).isEqualTo(Duration.ofSeconds(10));
        assertThat(p.baseDelay(2_000)).isEqualTo(Duration.ofSeconds(10));
        assertThat(p.baseDelay(Integer.MAX_VALUE)).isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void baseDelayWithAFractionalMultiplierTruncatesToWholeMilliseconds() {
        RetryPolicy p = policy(100, 1.5, 2_000, 0.0, 1);
        long[] expected = {100, 150, 225, 337, 506, 759, 1_139, 1_708, 2_000, 2_000};
        for (int n = 1; n <= expected.length; n++) {
            assertThat(p.baseDelay(n)).as("attempt " + n).isEqualTo(Duration.ofMillis(expected[n - 1]));
        }
    }

    @Test
    void multiplierOneOrCapEqualToInitialGivesAConstantDelay() {
        RetryPolicy flat = policy(250, 1.0, 60_000, 0.5, 1);
        RetryPolicy capped = policy(500, 2.0, 500, 0.5, 1);
        for (int n = 1; n <= 20; n++) {
            assertThat(flat.baseDelay(n)).isEqualTo(Duration.ofMillis(250));
            assertThat(capped.baseDelay(n)).isEqualTo(Duration.ofMillis(500));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
    void attemptNumbersBelowOneAreRejected(int attempt) {
        RetryPolicy p = policy(100, 2.0, 10_000, 0.5, 1);
        assertThatThrownBy(() -> p.baseDelay(attempt))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("failedAttempt must be >= 1");
        assertThatThrownBy(() -> p.backoff(attempt)).isInstanceOf(IllegalArgumentException.class);
    }

    // -------------------------------------------------------------------------------- backoff

    static Stream<Config> bandConfigs() {
        return Stream.of(
                new Config(100, 2.0, 10_000, 0.5),     // "equal jitter"
                new Config(1_000, 3.0, 300_000, 0.25),
                new Config(10, 2.0, 1_000, 0.9),       // heavy jitter, tiny delays
                new Config(250, 1.0, 250, 0.5));       // constant base
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("bandConfigs")
    void backoffStaysWithinTheJitterBandOverTenThousandSamplesPerAttempt(Config c) {
        RetryPolicy p = c.policy(new Random(20_260_101L));
        for (int attempt = 1; attempt <= c.attemptsPastCap(); attempt++) {
            long base = p.baseDelay(attempt).toMillis();
            BigDecimal lower = lowerBound(base, c.jitter());
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            for (int i = 0; i < SAMPLES; i++) {
                long d = p.backoff(attempt).toMillis();
                if (BigDecimal.valueOf(d).compareTo(lower) < 0 || d > base) {
                    throw new AssertionError("attempt " + attempt + ": delay " + d + " ms outside [" + lower + ", "
                            + base + "] ms");
                }
                min = Math.min(min, d);
                max = Math.max(max, d);
            }
            // The whole band is used: jitter is really applied and not biased towards one end.
            double span = base - lower.doubleValue();
            assertThat((double) min).as("attempt %d min", attempt).isLessThanOrEqualTo(lower.doubleValue() + 0.05 * span + 1);
            assertThat((double) max).as("attempt %d max", attempt).isGreaterThanOrEqualTo(base - 0.05 * span - 1);
        }
    }

    /**
     * With {@code base = 103 ms} and {@code jitter = 0.25} the documented band is {@code [77.25 ms, 103 ms]}, so a
     * whole-millisecond delay must be at least 78 ms. Values rounded to the nearest millisecond can land on 77.
     */
    @Test
    void backoffNeverDropsBelowAFractionalLowerBound() {
        RetryPolicy p = policy(103, 2.0, 10_000, 0.25, 42);
        BigDecimal lower = lowerBound(p.baseDelay(1).toMillis(), 0.25);
        assertThat(lower).isEqualByComparingTo("77.25");

        int below = 0;
        long lowest = Long.MAX_VALUE;
        for (int i = 0; i < SAMPLES; i++) {
            long d = p.backoff(1).toMillis();
            lowest = Math.min(lowest, d);
            if (BigDecimal.valueOf(d).compareTo(lower) < 0) below++;
        }
        assertThat(below).as("samples below (1 - jitter) * base = %s ms (lowest %d ms) out of %d", lower, lowest,
                SAMPLES).isZero();
    }

    static Stream<Config> growingConfigs() {
        return Stream.of(
                new Config(100, 2.0, 60_000, 0.25),   // (1 - j) * m = 1.5
                new Config(100, 3.0, 600_000, 0.5),   // 1.5
                new Config(50, 1.5, 10_000, 0.2));    // 1.2
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("growingConfigs")
    void backoffStrictlyIncreasesBetweenAttemptsWhileBelowTheCap(Config c) {
        assertThat((1 - c.jitter()) * c.multiplier()).isGreaterThan(1.0);
        int last = c.lastAttemptBelowCap();
        assertThat(last).isGreaterThanOrEqualTo(5);
        RetryPolicy p = c.policy(new Random(99));
        for (int trial = 0; trial < 2_000; trial++) {
            long previous = 0;
            for (int attempt = 1; attempt <= last; attempt++) {
                long d = p.backoff(attempt).toMillis();
                if (d <= previous) {
                    throw new AssertionError("trial " + trial + ": attempt " + attempt + " delay " + d
                            + " ms is not greater than the previous " + previous + " ms");
                }
                previous = d;
            }
        }
    }

    @Test
    void zeroJitterGivesExactlyTheBaseDelay() {
        RetryPolicy p = policy(100, 2.0, 10_000, 0.0, 3);
        for (int round = 0; round < 100; round++) {
            for (int attempt = 1; attempt <= 12; attempt++) {
                assertThat(p.backoff(attempt)).isEqualTo(p.baseDelay(attempt));
            }
        }
    }

    @Test
    void sameSeedGivesTheSameSequenceAndADifferentSeedDoesNot() {
        assertThat(sequence(new Random(7))).isEqualTo(sequence(new Random(7)));
        assertThat(sequence(new SplittableRandom(7))).isEqualTo(sequence(new SplittableRandom(7)));
        assertThat(sequence(new Random(7))).isNotEqualTo(sequence(new Random(8)));
    }

    private static List<Duration> sequence(RandomGenerator random) {
        RetryPolicy p = new Config(100, 2.0, 10_000, 0.5).policy(random);
        List<Duration> out = new ArrayList<>();
        for (int i = 0; i < 200; i++) out.add(p.backoff(1 + i % 10));
        return out;
    }

    // --------------------------------------------------------------------------------- decide

    static Stream<Arguments> decisions() {
        int maxAttempts = 3;
        List<Arguments> cases = new ArrayList<>();
        for (FailureKind kind : FailureKind.values()) {
            for (int attempt : new int[] {1, 2, 3, 4, 10}) {
                Action expected = !kind.retryable() ? Action.FAIL
                        : attempt >= maxAttempts ? Action.DEAD_LETTER : Action.RETRY;
                cases.add(Arguments.of(kind, attempt, maxAttempts, expected));
            }
        }
        return cases.stream();
    }

    @ParameterizedTest(name = "{0} attempt {1}/{2} -> {3}")
    @MethodSource("decisions")
    void decideForEveryFailureKindAndAttemptPosition(FailureKind kind, int attempt, int maxAttempts, Action expected) {
        RetryPolicy p = policy(100, 2.0, 10_000, 0.5, 5);
        RetryDecision decision = p.decide(attempt, maxAttempts, kind);

        assertThat(decision.action()).isEqualTo(expected);
        if (expected == Action.RETRY) {
            long base = p.baseDelay(attempt).toMillis();
            assertThat(decision.delay()).isNotNull();
            assertThat(BigDecimal.valueOf(decision.delay().toMillis())).isGreaterThanOrEqualTo(lowerBound(base, 0.5));
            assertThat(decision.delay().toMillis()).isLessThanOrEqualTo(base);
        } else {
            assertThat(decision.delay()).isNull();
        }
    }

    @Test
    void retryDelayUsesTheFailedAttemptNumber() {
        RetryPolicy p = policy(100, 2.0, 10_000, 0.0, 5);
        assertThat(p.decide(1, 10, FailureKind.RETRYABLE).delay()).isEqualTo(Duration.ofMillis(100));
        assertThat(p.decide(2, 10, FailureKind.TIMEOUT).delay()).isEqualTo(Duration.ofMillis(200));
        assertThat(p.decide(4, 10, FailureKind.LEASE_EXPIRED).delay()).isEqualTo(Duration.ofMillis(800));
        assertThat(p.decide(9, 10, FailureKind.WORKER_SHUTDOWN).delay()).isEqualTo(Duration.ofMillis(10_000));
    }

    @Test
    void singleAttemptJobsDeadLetterOnARetryableFailureAndFailOnAPermanentOne() {
        RetryPolicy p = policy(100, 2.0, 10_000, 0.5, 5);
        assertThat(p.decide(1, 1, FailureKind.RETRYABLE).action()).isEqualTo(Action.DEAD_LETTER);
        assertThat(p.decide(1, 1, FailureKind.NON_RETRYABLE).action()).isEqualTo(Action.FAIL);
    }

    // ---------------------------------------------------------------------------- constructor

    @Test
    void rejectsNullArguments() {
        RandomGenerator r = new Random(1);
        assertThatThrownBy(() -> new RetryPolicy(null, 2.0, Duration.ofSeconds(1), 0.5, r))
                .isInstanceOf(NullPointerException.class).hasMessage("initialDelay");
        assertThatThrownBy(() -> new RetryPolicy(Duration.ofSeconds(1), 2.0, null, 0.5, r))
                .isInstanceOf(NullPointerException.class).hasMessage("maxDelay");
        assertThatThrownBy(() -> new RetryPolicy(Duration.ofSeconds(1), 2.0, Duration.ofSeconds(1), 0.5, null))
                .isInstanceOf(NullPointerException.class).hasMessage("random");
    }

    @Test
    void rejectsNonPositiveInitialDelay() {
        RandomGenerator r = new Random(1);
        assertThatThrownBy(() -> new RetryPolicy(Duration.ZERO, 2.0, Duration.ofSeconds(1), 0.5, r))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("initialDelay must be positive");
        assertThatThrownBy(() -> new RetryPolicy(Duration.ofMillis(-1), 2.0, Duration.ofSeconds(1), 0.5, r))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("initialDelay must be positive");
    }

    @Test
    void rejectsCapBelowInitialDelay() {
        assertThatThrownBy(() -> new RetryPolicy(Duration.ofSeconds(2), 2.0, Duration.ofMillis(1_999), 0.5,
                new Random(1))).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxDelay must be >= initialDelay");
    }

    @ParameterizedTest
    @ValueSource(doubles = {0.999, 0.0, -2.0, Double.NaN, Double.NEGATIVE_INFINITY})
    void rejectsMultiplierBelowOne(double multiplier) {
        assertThatThrownBy(() -> new RetryPolicy(Duration.ofSeconds(1), multiplier, Duration.ofSeconds(10), 0.5,
                new Random(1))).isInstanceOf(IllegalArgumentException.class).hasMessage("multiplier must be >= 1");
    }

    @ParameterizedTest
    @ValueSource(doubles = {-0.01, 1.0, 1.5, Double.NaN, Double.POSITIVE_INFINITY})
    void rejectsJitterOutsideZeroInclusiveToOneExclusive(double jitter) {
        assertThatThrownBy(() -> new RetryPolicy(Duration.ofSeconds(1), 2.0, Duration.ofSeconds(10), jitter,
                new Random(1))).isInstanceOf(IllegalArgumentException.class).hasMessage("jitter must be in [0, 1)");
    }

    @Test
    void acceptsBoundaryValuesAndExposesThem() {
        assertThatNoException().isThrownBy(() -> new RetryPolicy(Duration.ofNanos(1), 1.0, Duration.ofNanos(1), 0.0,
                new Random(1)));
        assertThatNoException().isThrownBy(() -> new RetryPolicy(Duration.ofSeconds(1), 1.0, Duration.ofSeconds(1),
                0.999, new Random(1)));

        RetryPolicy p = new RetryPolicy(Duration.ofMillis(150), 2.5, Duration.ofMinutes(2), 0.3, new Random(1));
        assertThat(p.initialDelay()).isEqualTo(Duration.ofMillis(150));
        assertThat(p.multiplier()).isEqualTo(2.5);
        assertThat(p.maxDelay()).isEqualTo(Duration.ofMinutes(2));
        assertThat(p.jitter()).isEqualTo(0.3);
    }
}
