package io.akasb.taskplatform.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.akasb.taskplatform.support.Await;
import io.akasb.taskplatform.support.PostgresTestProperties;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Rate limiting without Redis (the default when {@code taskplatform.rate-limit.enabled=true}): a per-instance
 * in-memory limiter behind the servlet filter, through the real HTTP stack on a random port. No Docker needed.
 * {@link AutoConfigureObservability} turns metrics export (and so {@code /actuator/prometheus}) back on.
 */
@AutoConfigureObservability(tracing = false)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "taskplatform.rate-limit.enabled=true",
        "taskplatform.rate-limit.requests-per-window=3",
        "taskplatform.rate-limit.window=2s",
        "taskplatform.workers.shutdown-grace=500ms"})
class InMemoryRateLimitIT {
    private static final long WINDOW_MILLIS = 2_000;
    private static final String SLEEP = "{\"type\":\"sleep\",\"payload\":{\"durationMs\":5}}";

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
    ObjectProvider<RateLimiter> rateLimiterBeans;

    @Autowired
    ObjectProvider<IdempotencyCache> idempotencyCacheBeans;

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        PostgresTestProperties.register(registry);
    }

    @AfterEach
    void closeClient() {
        http.close();
    }

    @Test
    void fourthSubmissionInAWindowGets429AndOtherClientsAndEndpointsAreUnaffected() {
        assertThat(rateLimiterBeans.getIfAvailable()).as("no Redis limiter without Redis").isNull();
        assertThat(idempotencyCacheBeans.getIfAvailable()).as("no cache without Redis").isNull();
        double rejectedBefore = rejected();

        Await.until("start of a fresh rate-limit window", Duration.ofSeconds(5),
                () -> System.currentTimeMillis() % WINDOW_MILLIS < 200);
        String id = null;
        for (int i = 1; i <= 3; i++) {
            HttpResponse<String> accepted = submit("in-memory-a");
            assertThat(accepted.statusCode()).as("submission %d: %s", i, accepted.body()).isEqualTo(201);
            id = json(accepted).path("id").asText();
        }
        HttpResponse<String> limited = submit("in-memory-a");
        HttpResponse<String> otherClient = submit("in-memory-b");
        HttpResponse<String> read = get("/v1/jobs/" + id);

        assertThat(limited.statusCode()).as(limited.body()).isEqualTo(429);
        assertThat(limited.headers().firstValue("Retry-After")).hasValueSatisfying(
                s -> assertThat(Long.parseLong(s)).isBetween(1L, 2L));
        assertThat(limited.headers().firstValue("Content-Type")).hasValueSatisfying(
                ct -> assertThat(ct).startsWith(RateLimitFilter.PROBLEM_JSON));
        JsonNode problem = json(limited);
        assertThat(problem.path("type").asText()).isEqualTo("about:blank");
        assertThat(problem.path("status").asInt()).isEqualTo(429);
        assertThat(problem.path("detail").asText()).contains("retry after");
        assertThat(otherClient.statusCode()).as(otherClient.body()).isEqualTo(201);
        assertThat(read.statusCode()).as("reads are not limited").isEqualTo(200);
        assertThat(rejected()).isEqualTo(rejectedBefore + 1);

        HttpResponse<String> health = get("/actuator/health");
        assertThat(health.statusCode()).isEqualTo(200);
        assertThat(get("/actuator/prometheus").body()).contains("taskplatform_ratelimit_rejected_total");
    }

    private double rejected() {
        return meters.get("taskplatform.ratelimit.rejected").counter().count();
    }

    private HttpResponse<String> submit(String clientId) {
        return send(HttpRequest.newBuilder(uri("/v1/jobs"))
                .timeout(Duration.ofSeconds(20))
                .header("Content-Type", "application/json")
                .header(RateLimitFilter.CLIENT_ID_HEADER, clientId)
                .POST(HttpRequest.BodyPublishers.ofString(SLEEP)));
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
