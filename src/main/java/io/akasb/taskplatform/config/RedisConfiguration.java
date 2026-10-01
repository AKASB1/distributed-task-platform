package io.akasb.taskplatform.config;

import io.akasb.taskplatform.api.IdempotencyCache;
import io.akasb.taskplatform.api.RateLimiter;
import io.akasb.taskplatform.cache.RedisClients;
import io.akasb.taskplatform.cache.RedisConnectionHolder;
import io.akasb.taskplatform.cache.RedisIdempotencyCache;
import io.akasb.taskplatform.cache.RedisRateLimiter;
import io.lettuce.core.RedisClient;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Optional Redis ({@code taskplatform.redis.enabled=true}): an idempotency cache in front of PostgreSQL and, with
 * {@code taskplatform.rate-limit.enabled=true}, rate-limit counters shared by all instances. Redis is never
 * authoritative and the service starts and keeps working without it: the connection is opened lazily, re-opened at
 * most every {@link #RECONNECT_INTERVAL} after a failure, and every Redis error falls back (cache miss, request
 * allowed). Uses Lettuce directly, so no Redis health indicator can turn {@code /actuator/health} DOWN.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "taskplatform.redis", name = "enabled", havingValue = "true")
public class RedisConfiguration {
    /** Minimum time between two connection attempts while Redis is unreachable. */
    static final Duration RECONNECT_INTERVAL = Duration.ofSeconds(3);
    private static final Logger log = LoggerFactory.getLogger(RedisConfiguration.class);

    @Bean(destroyMethod = "shutdown")
    RedisClient redisClient(TaskPlatformProperties props) {
        TaskPlatformProperties.Redis redis = props.getRedis();
        log.info("Redis enabled for idempotency cache and rate limits (timeout {} ms, not authoritative)",
                redis.getTimeout().toMillis());
        return RedisClients.create(redis.getUri(), redis.getTimeout());
    }

    /** Reconnect back-off runs on wall-clock time even if the application clock is replaced (tests). */
    @Bean(destroyMethod = "close")
    RedisConnectionHolder redisConnectionHolder(RedisClient client, TaskPlatformProperties props) {
        return RedisConnectionHolder.of(client, props.getRedis().getTimeout(), RECONNECT_INTERVAL, Clock.systemUTC());
    }

    @Bean
    IdempotencyCache redisIdempotencyCache(RedisConnectionHolder redis, TaskPlatformProperties props,
                                           MeterRegistry registry, Clock clock) {
        TaskPlatformProperties.Redis r = props.getRedis();
        return new RedisIdempotencyCache(redis, r.getKeyPrefix(), r.getIdempotencyTtl(), registry, clock);
    }

    @Bean
    @ConditionalOnProperty(prefix = "taskplatform.rate-limit", name = "enabled", havingValue = "true")
    RateLimiter redisRateLimiter(RedisConnectionHolder redis, TaskPlatformProperties props, Clock clock,
                                 MeterRegistry registry) {
        TaskPlatformProperties.RateLimit rl = props.getRateLimit();
        return new RedisRateLimiter(redis, props.getRedis().getKeyPrefix(), rl.getRequestsPerWindow(), rl.getWindow(),
                clock, registry);
    }
}
