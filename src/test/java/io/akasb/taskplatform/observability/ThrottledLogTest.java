package io.akasb.taskplatform.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.akasb.taskplatform.support.MutableClock;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class ThrottledLogTest {
    private final MutableClock clock = MutableClock.startingAt("2026-01-01T00:00:00Z");
    private final ThrottledLog log = new ThrottledLog(LoggerFactory.getLogger(ThrottledLogTest.class),
            Duration.ofSeconds(30), clock);

    @Test
    void logsAtMostOncePerInterval() {
        RuntimeException cause = new IllegalStateException("redis down");
        assertThat(log.warn("Redis failed", cause)).isTrue();
        assertThat(log.warn("Redis failed", cause)).isFalse();
        clock.advance(Duration.ofSeconds(29));
        assertThat(log.warn("Redis failed", cause)).isFalse();
        clock.advance(Duration.ofSeconds(1));
        assertThat(log.warn("Redis failed", cause)).as("30 s later").isTrue();
        assertThat(log.warn("Redis failed", null)).isFalse();
    }
}
