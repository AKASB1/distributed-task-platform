package io.akasb.taskplatform.observability;

import java.time.Duration;
import java.time.Instant;

public final class Latency {
    private Latency() {}

    public static Duration between(Instant start, Instant end) {
        if (end.isBefore(start)) throw new IllegalArgumentException("end precedes start");
        return Duration.between(start, end);
    }

    /** Like {@link #between} but returns {@link Duration#ZERO} for a missing start or a negative interval. */
    public static Duration nonNegative(Instant start, Instant end) {
        if (start == null || end == null || end.isBefore(start)) return Duration.ZERO;
        return Duration.between(start, end);
    }
}
