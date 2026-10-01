package io.akasb.taskplatform;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.akasb.taskplatform.support.Await;
import io.akasb.taskplatform.support.PostgresTestProperties;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** Boots the whole service on a random port against embedded PostgreSQL and runs one job end to end. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "taskplatform.heartbeat-interval=200ms",
        "taskplatform.reconciler.interval=100ms"})
class ApplicationIT {
    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();

    @LocalServerPort
    int port;

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        PostgresTestProperties.register(registry);
    }

    @Test
    void bootsAndRunsAJob() throws Exception {
        HttpResponse<String> health = http.send(HttpRequest.newBuilder(uri("/actuator/health")).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(health.statusCode()).isEqualTo(200);

        HttpResponse<String> created = http.send(HttpRequest.newBuilder(uri("/v1/jobs"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"type\":\"sleep\",\"payload\":{\"durationMs\":20}}"))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(created.statusCode()).as(created.body()).isEqualTo(201);
        String id = mapper.readTree(created.body()).get("id").asText();

        JsonNode done = Await.value("job succeeded", Duration.ofSeconds(20), () -> get("/v1/jobs/" + id),
                j -> "SUCCEEDED".equals(j.path("state").asText()));
        assertThat(done.path("deliveries")).hasSize(1);
        assertThat(done.path("deliveries").get(0).path("ackState").asText()).isEqualTo("ACKED");
    }

    private JsonNode get(String path) {
        try {
            return mapper.readTree(http.send(HttpRequest.newBuilder(uri(path)).build(),
                    HttpResponse.BodyHandlers.ofString()).body());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private URI uri(String path) {
        return URI.create("http://127.0.0.1:" + port + path);
    }
}
