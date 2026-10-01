package io.akasb.taskplatform.observability;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import org.slf4j.Logger;

/**
 * Logs a recurring warning at most once per interval, so an outage of an optional dependency produces one line every
 * interval instead of one per request. The next line that gets through says how many were suppressed. Thread-safe.
 */
public final class ThrottledLog {
    private final Logger log;
    private final Duration interval;
    private final Clock clock;
    private Instant nextAllowed = Instant.MIN;
    private long suppressed;

    public ThrottledLog(Logger log, Duration interval, Clock clock) {
        this.log = Objects.requireNonNull(log);
        this.interval = Objects.requireNonNull(interval);
        this.clock = Objects.requireNonNull(clock);
        if (interval.isNegative()) throw new IllegalArgumentException("interval must not be negative");
    }

    /**
     * Logs {@code message} at WARN unless a warning was logged less than one interval ago.
     *
     * @return {@code true} if the line was written
     */
    public boolean warn(String message, Throwable cause) {
        long skipped;
        synchronized (this) {
            Instant now = clock.instant();
            if (now.isBefore(nextAllowed)) {
                suppressed++;
                return false;
            }
            nextAllowed = now.plus(interval);
            skipped = suppressed;
            suppressed = 0;
        }
        String reason = cause == null ? "" : ": " + cause;
        if (skipped > 0) {
            log.warn("{}{} ({} similar warnings suppressed in the last {} s)", message, reason, skipped,
                    interval.toSeconds());
        } else {
            log.warn("{}{}", message, reason);
        }
        return true;
    }
}
