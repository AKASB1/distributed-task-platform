package io.akasb.taskplatform.domain;

import java.time.Duration;
import java.util.Objects;
import java.util.random.RandomGenerator;

/**
 * Capped exponential backoff with bounded jitter.
 *
 * <p>For the n-th failed attempt (n ≥ 1): {@code base = min(maxDelay, initialDelay · multiplier^(n-1))} and
 * {@code delay = base · (1 − jitter · U)} with U uniform in [0, 1). So the delay never exceeds the cap, never drops
 * below {@code (1 − jitter) · base}, and strictly grows between attempts while {@code (1 − jitter) · multiplier > 1}
 * and the cap is not reached. {@code jitter = 0.5} is the "equal jitter" scheme.
 *
 * <p>The random source is injected so tests can use a seeded generator.
 */
public final class RetryPolicy {
    private final Duration initialDelay;
    private final double multiplier;
    private final Duration maxDelay;
    private final double jitter;
    private final RandomGenerator random;

    public RetryPolicy(Duration initialDelay, double multiplier, Duration maxDelay, double jitter,
                       RandomGenerator random) {
        this.initialDelay = Objects.requireNonNull(initialDelay, "initialDelay");
        this.maxDelay = Objects.requireNonNull(maxDelay, "maxDelay");
        this.random = Objects.requireNonNull(random, "random");
        if (initialDelay.isNegative() || initialDelay.isZero()) throw new IllegalArgumentException("initialDelay must be positive");
        if (maxDelay.compareTo(initialDelay) < 0) throw new IllegalArgumentException("maxDelay must be >= initialDelay");
        if (!(multiplier >= 1.0)) throw new IllegalArgumentException("multiplier must be >= 1");
        if (!(jitter >= 0.0 && jitter < 1.0)) throw new IllegalArgumentException("jitter must be in [0, 1)");
        this.multiplier = multiplier;
        this.jitter = jitter;
    }

    /** Upper bound (no jitter) of the delay after the n-th failed attempt. */
    public Duration baseDelay(int failedAttempt) {
        if (failedAttempt < 1) throw new IllegalArgumentException("failedAttempt must be >= 1");
        double millis = initialDelay.toMillis() * Math.pow(multiplier, failedAttempt - 1);
        long capped = (long) Math.min(maxDelay.toMillis(), millis);
        return Duration.ofMillis(Math.max(1, capped));
    }

    /** Jittered delay after the n-th failed attempt, in [(1 − jitter)·base, base]. */
    public Duration backoff(int failedAttempt) {
        long base = baseDelay(failedAttempt).toMillis();
        double u;
        synchronized (random) { // RandomGenerator implementations are not all thread-safe
            u = random.nextDouble();
        }
        // ceil (not round) so the result never drops below (1 - jitter) * base
        long delay = (long) Math.ceil(base * (1.0 - jitter * u));
        return Duration.ofMillis(Math.max(1, Math.min(base, delay)));
    }

    /** What happens to a job whose attempt number {@code attempt} (1-based) failed with {@code kind}. */
    public RetryDecision decide(int attempt, int maxAttempts, FailureKind kind) {
        if (!kind.retryable()) return RetryDecision.fail();
        if (attempt >= maxAttempts) return RetryDecision.deadLetter();
        return RetryDecision.retryAfter(backoff(attempt));
    }

    public Duration initialDelay() { return initialDelay; }
    public double multiplier() { return multiplier; }
    public Duration maxDelay() { return maxDelay; }
    public double jitter() { return jitter; }

    /** Outcome of {@link #decide}. {@code delay} is set only for {@link Action#RETRY}. */
    public record RetryDecision(Action action, Duration delay) {
        public enum Action { RETRY, FAIL, DEAD_LETTER }

        static RetryDecision retryAfter(Duration delay) { return new RetryDecision(Action.RETRY, delay); }
        static RetryDecision fail() { return new RetryDecision(Action.FAIL, null); }
        static RetryDecision deadLetter() { return new RetryDecision(Action.DEAD_LETTER, null); }
    }
}
