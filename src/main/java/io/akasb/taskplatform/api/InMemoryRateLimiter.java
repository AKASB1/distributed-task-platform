package io.akasb.taskplatform.api;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Fixed-window rate limiter kept in this process (used when rate limiting is on and Redis is not). Each instance of
 * the service counts on its own, so with N instances a client may get up to N times the limit.
 *
 * <p>Only the counters of the current window are kept: when the window moves on, the whole map is replaced, so
 * memory is bounded by the number of distinct clients seen in one window.
 */
public final class InMemoryRateLimiter implements RateLimiter {
    private final int requestsPerWindow;
    private final FixedWindow window;
    private final Clock clock;
    private final AtomicReference<Counts> current = new AtomicReference<>(new Counts(Long.MIN_VALUE));

    public InMemoryRateLimiter(int requestsPerWindow, Duration window, Clock clock) {
        if (requestsPerWindow < 1) throw new IllegalArgumentException("requestsPerWindow must be at least 1");
        this.requestsPerWindow = requestsPerWindow;
        this.window = new FixedWindow(window);
        this.clock = Objects.requireNonNull(clock);
    }

    @Override
    public Decision tryAcquire(String clientKey) {
        Objects.requireNonNull(clientKey, "clientKey");
        long now = clock.millis();
        long index = window.index(now);
        Counts counts = countsFor(index);
        int n = counts.perClient.computeIfAbsent(clientKey, k -> new AtomicInteger()).incrementAndGet();
        return n <= requestsPerWindow ? Decision.allow() : Decision.reject(window.untilNext(now));
    }

    private Counts countsFor(long index) {
        while (true) {
            Counts counts = current.get();
            // A clock that steps back keeps counting in the newer window instead of resetting the counters.
            if (counts.index >= index) return counts;
            Counts fresh = new Counts(index);
            if (current.compareAndSet(counts, fresh)) return fresh;
        }
    }

    private static final class Counts {
        final long index;
        final ConcurrentHashMap<String, AtomicInteger> perClient = new ConcurrentHashMap<>();

        Counts(long index) {
            this.index = index;
        }
    }
}
