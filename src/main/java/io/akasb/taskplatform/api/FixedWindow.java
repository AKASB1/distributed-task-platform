package io.akasb.taskplatform.api;

import java.time.Duration;
import java.util.Objects;

/**
 * Fixed rate-limit windows aligned to the Unix epoch: window {@code i} covers
 * {@code [i * length, (i + 1) * length)} in epoch milliseconds. Every instance (and every limiter backend) computes
 * the same window index for the same instant, so counters kept in a shared store line up across processes.
 */
public record FixedWindow(Duration length) {

    public FixedWindow {
        Objects.requireNonNull(length, "length");
        if (length.toMillis() < 1) throw new IllegalArgumentException("window must be at least 1 ms");
    }

    public long index(long epochMillis) {
        return Math.floorDiv(epochMillis, length.toMillis());
    }

    /** Time from {@code epochMillis} to the start of the next window (always positive). */
    public Duration untilNext(long epochMillis) {
        long millis = length.toMillis();
        return Duration.ofMillis((index(epochMillis) + 1) * millis - epochMillis);
    }
}
