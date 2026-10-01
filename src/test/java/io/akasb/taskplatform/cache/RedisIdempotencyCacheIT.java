package io.akasb.taskplatform.cache;

import static org.assertj.core.api.Assertions.assertThat;

import io.akasb.taskplatform.support.Await;
import io.akasb.taskplatform.support.RedisTestContainers;
import io.lettuce.core.KillArgs;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** {@link RedisIdempotencyCache} against a real Redis 7, including failure and reconnection. Needs Docker. */
@Testcontainers(disabledWithoutDocker = true)
class RedisIdempotencyCacheIT {
    private static final Duration TIMEOUT = Duration.ofMillis(500);
    private static final Duration TTL = Duration.ofMinutes(10);

    @Container
    static final GenericContainer<?> REDIS = RedisTestContainers.redis();

    private final MeterRegistry registry = new SimpleMeterRegistry();
    private final String prefix = "it-" + UUID.randomUUID() + ":";
    private RedisClient client;
    private RedisConnectionHolder holder;
    private RedisIdempotencyCache cache;
    private StatefulRedisConnection<String, String> admin;

    @BeforeEach
    void setUp() {
        client = RedisClients.create(RedisTestContainers.uri(REDIS), TIMEOUT);
        holder = RedisConnectionHolder.of(client, TIMEOUT, Duration.ofMillis(200), Clock.systemUTC());
        cache = new RedisIdempotencyCache(holder, prefix, TTL, registry, Clock.systemUTC());
        admin = client.connect();
    }

    @AfterEach
    void tearDown() {
        admin.close();
        holder.close();
        client.shutdown();
    }

    private double errors(String operation) {
        return registry.get(RedisFailures.ERRORS).tag("operation", operation).counter().count();
    }

    private double lookups(String result) {
        return registry.get(RedisIdempotencyCache.LOOKUPS).tag("result", result).counter().count();
    }

    @Test
    void remembersTheJobIdUnderThePrefixedKeyWithTheTtl() {
        UUID id = UUID.randomUUID();
        cache.remember("order-1", id);

        RedisCommands<String, String> redis = admin.sync();
        assertThat(cache.redisKey("order-1")).isEqualTo(prefix + "idem:order-1");
        assertThat(redis.get(prefix + "idem:order-1")).isEqualTo(id.toString());
        assertThat(redis.pttl(prefix + "idem:order-1")).isPositive().isLessThanOrEqualTo(TTL.toMillis());
        assertThat(cache.find("order-1")).contains(id);
        assertThat(lookups("hit")).isEqualTo(1);
    }

    @Test
    void unknownKeyIsAMiss() {
        assertThat(cache.find("never-seen")).isEmpty();
        assertThat(lookups("miss")).isEqualTo(1);
        assertThat(errors(RedisIdempotencyCache.FIND)).isZero();
    }

    @Test
    void entryThatIsNotAJobIdIsAMiss() {
        admin.sync().set(prefix + "idem:garbage", "not-a-uuid");
        assertThat(cache.find("garbage")).isEmpty();
    }

    @Test
    void rememberOverwritesAndRefreshesTheEntry() {
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        cache.remember("k", first);
        admin.sync().pexpire(prefix + "idem:k", 5_000);

        cache.remember("k", second);

        assertThat(cache.find("k")).contains(second);
        assertThat(admin.sync().pttl(prefix + "idem:k")).isGreaterThan(5_000);
    }

    @Test
    void reconnectsAfterTheServerClosesTheConnection() {
        UUID id = UUID.randomUUID();
        cache.remember("k", id);
        assertThat(holder.isConnected()).isTrue();

        long killed = admin.sync().clientKill(KillArgs.Builder.typeNormal().skipme());
        assertThat(killed).isGreaterThanOrEqualTo(1);

        Optional<UUID> found = Await.value("lookup works again after reconnecting", Duration.ofSeconds(10),
                () -> cache.find("k"), Optional::isPresent);
        assertThat(found).contains(id);
        assertThat(holder.isConnected()).isTrue();
    }

    @Test
    void unreachableRedisAtStartupIsAMissAndNeverThrows() {
        RedisClient downClient = clientFor(RedisTestContainers.unreachableUri());
        try (RedisConnectionHolder down = RedisConnectionHolder.of(downClient, TIMEOUT, Duration.ofSeconds(3),
                Clock.systemUTC())) {
            MeterRegistry meters = new SimpleMeterRegistry();
            RedisIdempotencyCache unreachable = new RedisIdempotencyCache(down, prefix, TTL, meters,
                    Clock.systemUTC());

            assertThat(unreachable.find("k")).isEmpty();
            unreachable.remember("k", UUID.randomUUID());
            assertThat(unreachable.find("k")).isEmpty();

            assertThat(meters.get(RedisFailures.ERRORS).tag("operation", RedisIdempotencyCache.FIND).counter().count())
                    .isEqualTo(2);
            assertThat(meters.get(RedisFailures.ERRORS).tag("operation", RedisIdempotencyCache.REMEMBER).counter()
                    .count()).isEqualTo(1);
        } finally {
            downClient.shutdown();
        }
    }

    @Test
    void failsOpenWhenRedisStops() {
        GenericContainer<?> own = RedisTestContainers.redis();
        own.start();
        RedisClient ownClient = clientFor(RedisTestContainers.uri(own));
        try (RedisConnectionHolder ownHolder = RedisConnectionHolder.of(ownClient, TIMEOUT, Duration.ofSeconds(3),
                Clock.systemUTC())) {
            RedisIdempotencyCache ownCache = new RedisIdempotencyCache(ownHolder, prefix, TTL, registry,
                    Clock.systemUTC());
            UUID id = UUID.randomUUID();
            ownCache.remember("k", id);
            assertThat(ownCache.find("k")).contains(id);

            own.stop();

            long start = System.nanoTime();
            for (int i = 0; i < 20; i++) {
                assertThat(ownCache.find("k")).isEmpty();
                ownCache.remember("k", id);
            }
            Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
            assertThat(errors(RedisIdempotencyCache.FIND)).isEqualTo(20);
            assertThat(errors(RedisIdempotencyCache.REMEMBER)).isEqualTo(20);
            assertThat(elapsed).as("back-off: one slow failure, then fast misses").isLessThan(Duration.ofSeconds(5));
        } finally {
            own.stop();
            ownClient.shutdown();
        }
    }

    private RedisClient clientFor(String uri) {
        return RedisClients.create(uri, TIMEOUT);
    }
}
