package io.akasb.taskplatform.observability;

import java.time.Duration;
import java.time.Instant;

public final class Latency {
    private Latency() {}
    public static Duration between(Instant start, Instant end) {
        if (end.isBefore(start)) throw new IllegalArgumentException("end precedes start");
        return Duration.between(start, end);
    }
}
