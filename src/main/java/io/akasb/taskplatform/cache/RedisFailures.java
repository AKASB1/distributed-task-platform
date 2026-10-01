package io.akasb.taskplatform.cache;

import io.akasb.taskplatform.observability.ThrottledLog;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;

/**
 * Records a Redis operation that failed and was answered by the fallback: increments
 * {@code taskplatform.redis.errors{operation}} (Prometheus {@code taskplatform_redis_errors_total}) and logs at WARN at
 * most once per {@link #WARN_INTERVAL}.
 */
final class RedisFailures {
    static final String ERRORS = "taskplatform.redis.errors";
    static final Duration WARN_INTERVAL = Duration.ofSeconds(30);

    private final Map<String, Counter> counters;
    private final ThrottledLog warnings;

    /** Registers one counter per operation at zero, so the series exist before the first failure. */
    RedisFailures(MeterRegistry registry, Logger log, Clock clock, String... operations) {
        Objects.requireNonNull(registry);
        this.counters = Stream.of(operations).collect(Collectors.toUnmodifiableMap(Function.identity(),
                op -> Counter.builder(ERRORS)
                        .description("Redis operations that failed or timed out and were answered by the fallback")
                        .tag("operation", op)
                        .register(registry)));
        this.warnings = new ThrottledLog(log, WARN_INTERVAL, clock);
    }

    void record(String operation, String fallback, RuntimeException e) {
        Counter counter = counters.get(operation);
        if (counter == null) throw new IllegalArgumentException("unknown operation " + operation);
        counter.increment();
        warnings.warn("Redis " + operation + " failed, " + fallback, e);
    }
}
