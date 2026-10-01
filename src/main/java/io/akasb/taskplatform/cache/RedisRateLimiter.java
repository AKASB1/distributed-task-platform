package io.akasb.taskplatform.cache;

import io.akasb.taskplatform.api.FixedWindow;
import io.akasb.taskplatform.api.RateLimiter;
import io.lettuce.core.ScriptOutputType;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fixed-window rate limiter shared by every instance through Redis: {@code INCR <keyPrefix>rl:<client>:<window index>},
 * with {@code PEXPIRE} on the first increment of a window, both in one Lua script so they are atomic and a crash cannot
 * leave a counter without expiry. A request is allowed while the count is at most {@code requestsPerWindow}; a
 * rejected one is told to retry at the start of the next window. Window indexes come from the injected clock and are
 * aligned to the epoch ({@link FixedWindow}), so instances agree as long as their clocks roughly do.
 *
 * <p>Fails open: an error or timeout allows the request and is counted in
 * {@code taskplatform.redis.errors{operation=rate_limit}}.
 */
public final class RedisRateLimiter implements RateLimiter {
    static final String OPERATION = "rate_limit";
    /** KEYS[1] = counter, ARGV[1] = expiry in ms. Returns the count after the increment. */
    static final String INCREMENT_SCRIPT = """
            local n = redis.call('INCR', KEYS[1])
            if n == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
            return n""";
    private static final Logger log = LoggerFactory.getLogger(RedisRateLimiter.class);

    private final RedisConnectionHolder redis;
    private final String keyPrefix;
    private final int requestsPerWindow;
    private final FixedWindow window;
    private final String expiryMillis;
    private final Clock clock;
    private final RedisFailures failures;

    public RedisRateLimiter(RedisConnectionHolder redis, String keyPrefix, int requestsPerWindow, Duration window,
                            Clock clock, MeterRegistry registry) {
        if (requestsPerWindow < 1) throw new IllegalArgumentException("requestsPerWindow must be at least 1");
        this.redis = Objects.requireNonNull(redis);
        this.keyPrefix = Objects.requireNonNull(keyPrefix);
        this.requestsPerWindow = requestsPerWindow;
        this.window = new FixedWindow(window);
        // Twice the window: the counter outlives its window even if instance clocks disagree a little.
        this.expiryMillis = Long.toString(2 * window.toMillis());
        this.clock = Objects.requireNonNull(clock);
        this.failures = new RedisFailures(registry, log, clock, OPERATION);
    }

    /** The Redis key that counts {@code clientKey}'s requests in window {@code windowIndex}. */
    public String redisKey(String clientKey, long windowIndex) {
        return keyPrefix + "rl:" + clientKey + ":" + windowIndex;
    }

    @Override
    public Decision tryAcquire(String clientKey) {
        Objects.requireNonNull(clientKey, "clientKey");
        long now = clock.millis();
        String key = redisKey(clientKey, window.index(now));
        long count;
        try {
            Long n = redis.execute(c -> c.<Long>eval(INCREMENT_SCRIPT, ScriptOutputType.INTEGER,
                    new String[] {key}, expiryMillis));
            count = Objects.requireNonNull(n, "INCR returned nothing");
        } catch (RuntimeException e) {
            failures.record(OPERATION, "request allowed", e);
            return Decision.allow();
        }
        return count <= requestsPerWindow ? Decision.allow() : Decision.reject(window.untilNext(now));
    }
}
