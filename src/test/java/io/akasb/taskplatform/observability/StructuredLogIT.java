package io.akasb.taskplatform.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.akasb.taskplatform.support.Await;
import io.akasb.taskplatform.support.PostgresTestProperties;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Captures stdout while a job runs and checks that the worker's structured (Logstash JSON) log lines carry the job
 * context from the MDC: {@code jobId}, {@code attempt} (as a string), {@code queue} and {@code workerId}.
 */
@ExtendWith(OutputCaptureExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "logging.structured.format.console=logstash",
        "taskplatform.heartbeat-interval=100ms",
        "taskplatform.lease-duration=2s",
        "taskplatform.reconciler.interval=50ms",
        "taskplatform.retry.initial-delay=50ms",
        "taskplatform.retry.max-delay=200ms",
        "taskplatform.retry.seed=7",
        "taskplatform.workers.poll-timeout=200ms",
        "taskplatform.workers.shutdown-grace=500ms",
        "taskplatform.workers.pools[0].name=default",
        "taskplatform.workers.pools[0].queue=default",
        "taskplatform.workers.pools[0].concurrency=2",
        "taskplatform.workers.pools[1].name=slow",
        "taskplatform.workers.pools[1].queue=slow",
        "taskplatform.workers.pools[1].concurrency=1"})
class StructuredLogIT {
    private static final Duration WAIT = Duration.ofSeconds(20);

    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @LocalServerPort
    int port;

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        PostgresTestProperties.register(registry);
    }

    @AfterEach
    void closeClient() {
        http.close();
    }

    @Test
    void workerLogLinesCarryTheJobContext(CapturedOutput output) {
        HttpResponse<String> created = send(HttpRequest.newBuilder(uri("/v1/jobs"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"type\":\"sleep\",\"payload\":{\"durationMs\":20}}")));
        assertThat(created.statusCode()).as(created.body()).isEqualTo(201);
        String id = json(created.body()).path("id").asText();
        Await.until("job " + id + " succeeded", WAIT, () -> "SUCCEEDED".equals(
                json(send(HttpRequest.newBuilder(uri("/v1/jobs/" + id)).GET()).body()).path("state").asText()));

        String line = Await.value("a JSON log line with jobId, attempt and workerId", WAIT,
                () -> workerLine(output.getOut(), id), Optional::isPresent).orElseThrow();

        assertThat(line).contains("\"jobId\":\"" + id + "\"").contains("\"attempt\":\"1\"").contains("\"workerId\":");
        JsonNode event = json(line);
        assertThat(event.path("jobId").asText()).isEqualTo(id);
        assertThat(event.path("attempt").isTextual()).isTrue();
        assertThat(event.path("attempt").asText()).isEqualTo("1");
        assertThat(event.path("workerId").asText()).startsWith("default@");
        assertThat(event.path("queue").asText()).isEqualTo("default");
        assertThat(event.path("message").asText()).isNotBlank();
        assertThat(event.path("level").asText()).isNotBlank();
        assertThat(event.has("@timestamp")).isTrue();
    }

    /** The first stdout line that is a JSON object logged inside the worker's MDC scope for {@code jobId}. */
    private Optional<String> workerLine(String out, String jobId) {
        List<String> candidates = out.lines()
                .map(String::trim)
                .filter(l -> l.startsWith("{") && l.contains(jobId) && l.contains("\"workerId\""))
                .toList();
        for (String l : candidates) {
            JsonNode event;
            try {
                event = mapper.readTree(l);
            } catch (IOException e) {
                continue; // an interleaved or partial line
            }
            if (jobId.equals(event.path("jobId").asText()) && "1".equals(event.path("attempt").asText())
                    && !event.path("workerId").asText().isEmpty()) {
                return Optional.of(l);
            }
        }
        return Optional.empty();
    }

    private URI uri(String path) {
        return URI.create("http://127.0.0.1:" + port + path);
    }

    private HttpResponse<String> send(HttpRequest.Builder request) {
        try {
            return http.send(request.timeout(Duration.ofSeconds(15)).build(), HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private JsonNode json(String body) {
        try {
            return mapper.readTree(body);
        } catch (IOException e) {
            throw new UncheckedIOException("not JSON: " + body, e);
        }
    }
}
