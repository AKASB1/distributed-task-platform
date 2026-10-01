package io.akasb.taskplatform;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.akasb.taskplatform.api.IdempotencyCache;
import io.akasb.taskplatform.api.JobController;
import io.akasb.taskplatform.api.RateLimitFilter;
import io.akasb.taskplatform.api.RateLimiter;
import io.akasb.taskplatform.cache.RedisClients;
import io.akasb.taskplatform.cache.RedisIdempotencyCache;
import io.akasb.taskplatform.cache.RedisRateLimiter;
import io.akasb.taskplatform.support.Await;
import io.akasb.taskplatform.support.PostgresTestProperties;
import io.akasb.taskplatform.support.RedisTestContainers;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The whole service with Redis enabled (embedded PostgreSQL, Redis 7 in Testcontainers, real HTTP on a random port):
 * Redis answers idempotent replays and holds the rate-limit counters while it is up; once it is stopped, submissions
 * still succeed (fail open) and replays are still recognised by PostgreSQL, the source of truth. Health stays UP
 * throughout because no Redis health indicator exists. Needs Docker.
 *
 * <p>{@link AutoConfigureObservability} turns metrics export (and so {@code /actuator/prometheus}) back on.
 */
@Testcontainers(disabledWithoutDocker = true)
@AutoConfigureObservability(tracing = false)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "taskplatform.redis.enabled=true",
        "taskplatform.redis.timeout=500ms",
        "taskplatform.rate-limit.enabled=true",
        "taskplatform.rate-limit.requests-per-window=5",
        "taskplatform.rate-limit.window=2s",
        "taskplatform.heartbeat-interval=200ms",
        "taskplatform.reconciler.interval=100ms",
        "taskplatform.workers.shutdown-grace=500ms"})
class RedisEndToEndIT {
    private static final long WINDOW_MILLIS = 2_000;
    private static final String SLEEP = "{\"type\":\"sleep\",\"payload\":{\"durationMs\":5}}";

    @Container
    static final GenericContainer<?> REDIS = RedisTestContainers.redis();

    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @LocalServerPort
    int port;

    @Autowired
    MeterRegistry meters;

    @Autowired
    IdempotencyCache idempotencyCache;

    @Autowired
    RateLimiter rateLimiter;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        PostgresTestProperties.register(registry);
        registry.add("taskplatform.redis.uri", () -> RedisTestContainers.uri(REDIS));
    }

    @AfterEach
    void closeClient() {
        http.close();
    }

    @Test
    void redisAcceleratesWhileUpAndIsNotNeededWhenDown() {
        assertThat(idempotencyCache).isInstanceOf(RedisIdempotencyCache.class);
        assertThat(rateLimiter).isInstanceOf(RedisRateLimiter.class);
        assertHealthUp();

        // 1. An idempotent replay is answered from the Redis cache (and confirmed by PostgreSQL).
        String key = "e2e-" + UUID.randomUUID();
        HttpResponse<String> created = submit(SLEEP, key, "idem-client");
        assertThat(created.statusCode()).as(created.body()).isEqualTo(201);
        String id = json(created).path("id").asText();
        RedisClient client = RedisClients.create(RedisTestContainers.uri(REDIS), Duration.ofSeconds(2));
        try (StatefulRedisConnection<String, String> admin = client.connect()) {
            assertThat(admin.sync().get("taskplatform:idem:" + key)).as("cached job id").isEqualTo(id);
            assertThat(admin.sync().pttl("taskplatform:idem:" + key)).isPositive();

            double hitsBefore = cacheHits();
            HttpResponse<String> replay = submit(SLEEP, key, "idem-client");
            assertThat(replay.statusCode()).as(replay.body()).isEqualTo(200);
            assertThat(replay.headers().firstValue(JobController.IDEMPOTENT_REPLAYED)).hasValue("true");
            assertThat(json(replay).path("id").asText()).isEqualTo(id);
            assertThat(cacheHits()).as("answered via the cache").isEqualTo(hitsBefore + 1);

            // 2. The 6th quick submission of one client in a window is rejected; another client is not affected.
            awaitFreshWindow();
            for (int i = 1; i <= 5; i++) {
                HttpResponse<String> accepted = submit(SLEEP, null, "client-a");
                assertThat(accepted.statusCode()).as("submission %d: %s", i, accepted.body()).isEqualTo(201);
            }
            HttpResponse<String> limited = submit(SLEEP, null, "client-a");
            HttpResponse<String> otherClient = submit(SLEEP, null, "client-b");

            assertThat(limited.statusCode()).as(limited.body()).isEqualTo(429);
            assertThat(limited.headers().firstValue("Retry-After")).hasValueSatisfying(
                    s -> assertThat(Long.parseLong(s)).isBetween(1L, 2L));
            assertThat(limited.headers().firstValue("Content-Type")).hasValueSatisfying(
                    ct -> assertThat(ct).startsWith(RateLimitFilter.PROBLEM_JSON));
            JsonNode problem = json(limited);
            assertThat(problem.path("status").asInt()).isEqualTo(429);
            assertThat(problem.path("title").asText()).isEqualTo("Too Many Requests");
            assertThat(otherClient.statusCode()).as(otherClient.body()).isEqualTo(201);
            List<String> counters = admin.sync().keys("taskplatform:rl:id:client-a:*");
            assertThat(counters).as("one window counter in Redis").hasSize(1);
            assertThat(admin.sync().get(counters.get(0))).isEqualTo("6");
        } finally {
            client.shutdown();
        }
        assertHealthUp();

        // 3. Redis goes away: submissions still succeed and the database still recognises the replay.
        REDIS.stop();

        for (int i = 1; i <= 7; i++) {
            HttpResponse<String> accepted = submit(SLEEP, null, "client-c");
            assertThat(accepted.statusCode()).as("fail open, submission %d: %s", i, accepted.body()).isEqualTo(201);
        }
        HttpResponse<String> replayWithoutRedis = submit(SLEEP, key, "idem-client");
        assertThat(replayWithoutRedis.statusCode()).as(replayWithoutRedis.body()).isEqualTo(200);
        assertThat(json(replayWithoutRedis).path("id").asText()).isEqualTo(id);
        HttpResponse<String> fresh = submit(SLEEP, "e2e-" + UUID.randomUUID(), "idem-client");
        assertThat(fresh.statusCode()).as(fresh.body()).isEqualTo(201);

        assertThat(redisErrors("rate_limit")).isPositive();
        assertThat(redisErrors("idempotency_find")).isPositive();
        assertThat(redisErrors("idempotency_remember")).isPositive();
        assertHealthUp();
        assertThat(get("/actuator/prometheus").body())
                .contains("taskplatform_redis_errors_total")
                .contains("taskplatform_ratelimit_rejected_total");
    }

    /** Waits (polling) until the current 2 s window has just begun, so six quick requests fall into one window. */
    private static void awaitFreshWindow() {
        Await.until("start of a fresh rate-limit window", Duration.ofSeconds(5),
                () -> System.currentTimeMillis() % WINDOW_MILLIS < 200);
    }

    private double cacheHits() {
        return meters.get("taskplatform.redis.idempotency.lookups").tag("result", "hit").counter().count();
    }

    private double redisErrors(String operation) {
        return meters.get("taskplatform.redis.errors").tag("operation", operation).counter().count();
    }

    private void assertHealthUp() {
        HttpResponse<String> health = get("/actuator/health");
        assertThat(health.statusCode()).as(health.body()).isEqualTo(200);
        assertThat(json(health).path("status").asText()).isEqualTo("UP");
    }

    private HttpResponse<String> submit(String body, String idempotencyKey, String clientId) {
        HttpRequest.Builder request = HttpRequest.newBuilder(uri("/v1/jobs"))
                .timeout(Duration.ofSeconds(20))
                .header("Content-Type", "application/json")
                .header(RateLimitFilter.CLIENT_ID_HEADER, clientId)
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (idempotencyKey != null) request.header(JobController.IDEMPOTENCY_KEY, idempotencyKey);
        return send(request);
    }

    private HttpResponse<String> get(String path) {
        return send(HttpRequest.newBuilder(uri(path)).timeout(Duration.ofSeconds(20)).GET());
    }

    private HttpResponse<String> send(HttpRequest.Builder request) {
        try {
            return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private JsonNode json(HttpResponse<String> response) {
        try {
            return mapper.readTree(response.body());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private URI uri(String path) {
        return URI.create("http://127.0.0.1:" + port + path);
    }
}
