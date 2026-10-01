package io.akasb.taskplatform.cache;

import static org.assertj.core.api.Assertions.assertThat;

import io.akasb.taskplatform.api.RateLimiter.Decision;
import io.akasb.taskplatform.support.MutableClock;
import io.akasb.taskplatform.support.RedisTestContainers;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** {@link RedisRateLimiter} against a real Redis 7: shared fixed windows, atomic counting, fail open. Needs Docker. */
@Testcontainers(disabledWithoutDocker = true)
class RedisRateLimiterIT {
    private static final Duration TIMEOUT = Duration.ofMillis(500);
    private static final Duration WINDOW = Duration.ofSeconds(2);
    /** A multiple of the window, so the clock starts exactly at a window boundary. */
    private static final Instant WINDOW_START = Instant.parse("2026-01-01T00:00:00Z");

    @Container
    static final GenericContainer<?> REDIS = RedisTestContainers.redis();

    private final MeterRegistry registry = new SimpleMeterRegistry();
    private final MutableClock clock = new MutableClock(WINDOW_START);
    private final String prefix = "it-" + UUID.randomUUID() + ":";
    private RedisClient client;
    private RedisConnectionHolder holder;
    private StatefulRedisConnection<String, String> admin;

    @BeforeEach
    void setUp() {
        client = RedisClients.create(RedisTestContainers.uri(REDIS), TIMEOUT);
        holder = RedisConnectionHolder.of(client, TIMEOUT, Duration.ofSeconds(3), Clock.systemUTC());
        admin = client.connect();
    }

    @AfterEach
    void tearDown() {
        admin.close();
        holder.close();
        client.shutdown();
    }

    private RedisRateLimiter limiter(int limit) {
        return new RedisRateLimiter(holder, prefix, limit, WINDOW, clock, registry);
    }

    private double errors() {
        return registry.get(RedisFailures.ERRORS).tag("operation", RedisRateLimiter.OPERATION).counter().count();
    }

    @Test
    void allowsUpToTheLimitThenRejectsWithTheTimeToTheNextWindow() {
        RedisRateLimiter limiter = limiter(3);
        long index = WINDOW_START.toEpochMilli() / WINDOW.toMillis();

        for (int i = 0; i < 3; i++) assertThat(limiter.tryAcquire("a")).isEqualTo(Decision.allow());
        clock.advance(Duration.ofMillis(800));
        Decision rejected = limiter.tryAcquire("a");

        assertThat(rejected.allowed()).isFalse();
        assertThat(rejected.retryAfter()).isEqualTo(Duration.ofMillis(1200));
        String key = prefix + "rl:a:" + index;
        assertThat(limiter.redisKey("a", index)).isEqualTo(key);
        assertThat(admin.sync().get(key)).isEqualTo("4");
        assertThat(admin.sync().pttl(key)).as("expires on its own").isPositive()
                .isLessThanOrEqualTo(2 * WINDOW.toMillis());
        assertThat(errors()).isZero();
    }

    @Test
    void theNextWindowStartsFromZeroUnderANewKey() {
        RedisRateLimiter limiter = limiter(2);
        long index = WINDOW_START.toEpochMilli() / WINDOW.toMillis();
        clock.advance(Duration.ofMillis(1999));
        limiter.tryAcquire("a");
        limiter.tryAcquire("a");
        assertThat(limiter.tryAcquire("a").allowed()).isFalse();

        clock.advance(Duration.ofMillis(1));
        assertThat(limiter.tryAcquire("a").allowed()).isTrue();
        assertThat(limiter.tryAcquire("a").allowed()).isTrue();
        assertThat(limiter.tryAcquire("a").retryAfter()).isEqualTo(WINDOW);
        assertThat(admin.sync().get(prefix + "rl:a:" + (index + 1))).isEqualTo("3");
    }

    @Test
    void clientsAreCountedSeparately() {
        RedisRateLimiter limiter = limiter(1);
        assertThat(limiter.tryAcquire("id:a").allowed()).isTrue();
        assertThat(limiter.tryAcquire("id:a").allowed()).isFalse();
        assertThat(limiter.tryAcquire("id:b").allowed()).isTrue();
        assertThat(limiter.tryAcquire("ip:127.0.0.1").allowed()).isTrue();
    }

    @Test
    void instancesSharingRedisShareTheLimit() {
        RedisRateLimiter first = limiter(4);
        try (RedisConnectionHolder otherHolder = RedisConnectionHolder.of(client, TIMEOUT, Duration.ofSeconds(3),
                Clock.systemUTC())) {
            RedisRateLimiter second = new RedisRateLimiter(otherHolder, prefix, 4, WINDOW, clock,
                    new SimpleMeterRegistry());
            int allowed = 0;
            for (int i = 0; i < 5; i++) {
                if (first.tryAcquire("a").allowed()) allowed++;
                if (second.tryAcquire("a").allowed()) allowed++;
            }
            assertThat(allowed).isEqualTo(4);
        }
    }

    @Test
    void exactlyTheLimitIsAllowedUnderConcurrency() throws Exception {
        int threads = 8;
        int perThread = 25;
        int limit = 100;
        RedisRateLimiter limiter = limiter(limit);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Integer>> results = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                results.add(pool.submit(() -> {
                    start.await();
                    int allowed = 0;
                    for (int i = 0; i < perThread; i++) {
                        if (limiter.tryAcquire("hot").allowed()) allowed++;
                    }
                    return allowed;
                }));
            }
            start.countDown();
            int total = 0;
            for (Future<Integer> f : results) total += f.get(60, TimeUnit.SECONDS);
            assertThat(total).isEqualTo(limit);
            assertThat(errors()).isZero();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void unreachableRedisAtStartupAllowsEveryRequest() {
        RedisClient downClient = RedisClients.create(RedisTestContainers.unreachableUri(), TIMEOUT);
        try (RedisConnectionHolder down = RedisConnectionHolder.of(downClient, TIMEOUT, Duration.ofSeconds(3),
                Clock.systemUTC())) {
            RedisRateLimiter limiter = new RedisRateLimiter(down, prefix, 1, WINDOW, clock, registry);
            for (int i = 0; i < 5; i++) assertThat(limiter.tryAcquire("a").allowed()).isTrue();
            assertThat(errors()).isEqualTo(5);
        } finally {
            downClient.shutdown();
        }
    }

    @Test
    void failsOpenAfterRedisStops() {
        GenericContainer<?> own = RedisTestContainers.redis();
        own.start();
        RedisClient ownClient = RedisClients.create(RedisTestContainers.uri(own), TIMEOUT);
        try (RedisConnectionHolder ownHolder = RedisConnectionHolder.of(ownClient, TIMEOUT, Duration.ofSeconds(3),
                Clock.systemUTC())) {
            RedisRateLimiter limiter = new RedisRateLimiter(ownHolder, prefix, 2, WINDOW, clock, registry);
            assertThat(limiter.tryAcquire("a").allowed()).isTrue();
            assertThat(limiter.tryAcquire("a").allowed()).isTrue();
            assertThat(limiter.tryAcquire("a").allowed()).as("limited while Redis is up").isFalse();

            own.stop();

            long start = System.nanoTime();
            for (int i = 0; i < 20; i++) {
                assertThat(limiter.tryAcquire("a").allowed()).as("request %d after the stop", i).isTrue();
            }
            Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
            assertThat(errors()).isEqualTo(20);
            assertThat(elapsed).as("back-off: one slow failure, then fast answers").isLessThan(Duration.ofSeconds(5));
        } finally {
            own.stop();
            ownClient.shutdown();
        }
    }
}
