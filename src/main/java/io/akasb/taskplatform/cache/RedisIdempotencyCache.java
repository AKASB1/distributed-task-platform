package io.akasb.taskplatform.cache;

import io.akasb.taskplatform.api.IdempotencyCache;
import io.lettuce.core.SetArgs;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Idempotency cache in Redis: key {@code <keyPrefix>idem:<idempotency key>}, value the job id, expiring after
 * {@code ttl}. Fails open: an error or timeout is a miss (lookups) or ignored (updates), counted in
 * {@code taskplatform.redis.errors{operation=idempotency_find|idempotency_remember}}. Answers are hints that
 * {@code JobService} verifies against PostgreSQL.
 *
 * <p>Lookups are counted in {@code taskplatform.redis.idempotency.lookups{result=hit|miss}}.
 */
public final class RedisIdempotencyCache implements IdempotencyCache {
    static final String FIND = "idempotency_find";
    static final String REMEMBER = "idempotency_remember";
    static final String LOOKUPS = "taskplatform.redis.idempotency.lookups";
    private static final Logger log = LoggerFactory.getLogger(RedisIdempotencyCache.class);

    private final RedisConnectionHolder redis;
    private final String keyPrefix;
    private final long ttlMillis;
    private final RedisFailures failures;
    private final Counter hits;
    private final Counter misses;

    public RedisIdempotencyCache(RedisConnectionHolder redis, String keyPrefix, Duration ttl, MeterRegistry registry,
                                 Clock clock) {
        this.redis = Objects.requireNonNull(redis);
        this.keyPrefix = Objects.requireNonNull(keyPrefix);
        Objects.requireNonNull(ttl, "ttl");
        if (ttl.toMillis() < 1) throw new IllegalArgumentException("ttl must be at least 1 ms");
        this.ttlMillis = ttl.toMillis();
        this.failures = new RedisFailures(registry, log, clock, FIND, REMEMBER);
        this.hits = lookups(registry, "hit");
        this.misses = lookups(registry, "miss");
    }

    /** The Redis key that holds the job id for {@code idempotencyKey}. */
    public String redisKey(String idempotencyKey) {
        return keyPrefix + "idem:" + idempotencyKey;
    }

    @Override
    public Optional<UUID> find(String key) {
        String value;
        try {
            value = redis.execute(c -> c.get(redisKey(key)));
        } catch (RuntimeException e) {
            failures.record(FIND, "treated as a cache miss", e);
            return Optional.empty();
        }
        if (value == null) {
            misses.increment();
            return Optional.empty();
        }
        try {
            UUID id = UUID.fromString(value);
            hits.increment();
            return Optional.of(id);
        } catch (IllegalArgumentException e) {
            log.debug("ignoring idempotency cache entry that is not a job id");
            misses.increment();
            return Optional.empty();
        }
    }

    @Override
    public void remember(String key, UUID jobId) {
        Objects.requireNonNull(jobId, "jobId");
        try {
            redis.execute(c -> c.set(redisKey(key), jobId.toString(), SetArgs.Builder.px(ttlMillis)));
        } catch (RuntimeException e) {
            failures.record(REMEMBER, "entry not cached", e);
        }
    }

    private static Counter lookups(MeterRegistry registry, String result) {
        return Counter.builder(LOOKUPS).description("Idempotency cache lookups that reached Redis")
                .tag("result", result).register(registry);
    }
}
