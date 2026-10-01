package io.akasb.taskplatform.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.akasb.taskplatform.api.JobService;
import io.akasb.taskplatform.api.SubmitJobRequest;
import io.akasb.taskplatform.dispatch.JobLifecycle;
import io.akasb.taskplatform.domain.Lease;
import io.akasb.taskplatform.support.Await;
import io.akasb.taskplatform.support.PostgresTestProperties;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Runs successful, retried, failed, dead-lettered and cancelled jobs plus one stale lease through the real service
 * and checks that {@code GET /actuator/prometheus} exposes every job-lifecycle and worker meter with the expected
 * labels.
 *
 * <p>The stale lease: a job on the pool-less queue {@code orphan} (allowed by {@code require-served-queue=false}) is
 * claimed directly through {@link JobLifecycle#claim} by a "ghost" worker that never heartbeats; the running
 * reconciler reclaims it after {@code lease-duration + lease-grace}. The two tests share one context and only use
 * lower bounds on meters they both touch, so their order does not matter.
 *
 * <p>{@link AutoConfigureObservability} is required: Spring Boot tests disable metrics export otherwise, and then
 * there is no Prometheus registry and {@code /actuator/prometheus} answers 404.
 */
@AutoConfigureObservability(tracing = false)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "taskplatform.heartbeat-interval=100ms",
        "taskplatform.lease-duration=2s",
        "taskplatform.reconciler.interval=50ms",
        "taskplatform.reconciler.lease-grace=100ms",
        "taskplatform.retry.initial-delay=50ms",
        "taskplatform.retry.max-delay=200ms",
        "taskplatform.retry.seed=7",
        "taskplatform.api.require-served-queue=false",
        "taskplatform.workers.poll-timeout=200ms",
        "taskplatform.workers.shutdown-grace=500ms",
        "taskplatform.workers.pools[0].name=default",
        "taskplatform.workers.pools[0].queue=default",
        "taskplatform.workers.pools[0].concurrency=2",
        "taskplatform.workers.pools[1].name=slow",
        "taskplatform.workers.pools[1].queue=slow",
        "taskplatform.workers.pools[1].concurrency=1"})
class MetricsIT {
    private static final Duration WAIT = Duration.ofSeconds(20);
    private static final Pattern SAMPLE = Pattern.compile("^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\\{(.*)})?\\s+(\\S+)");
    private static final Pattern LABEL = Pattern.compile("([a-zA-Z_][a-zA-Z0-9_]*)=\"((?:[^\"\\\\]|\\\\.)*)\"");

    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @LocalServerPort
    int port;

    @Autowired
    JobLifecycle lifecycle;

    @Autowired
    JobService jobService;

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        PostgresTestProperties.register(registry);
    }

    @AfterEach
    void closeClient() {
        http.close();
    }

    @Test
    void prometheusScrapeCoversTheJobLifecycle() {
        String succeeded = submit("{\"type\":\"sleep\",\"payload\":{\"durationMs\":10}}");
        String retried = submit("{\"type\":\"flaky\",\"payload\":{\"durationMs\":10,\"failAttempts\":1}}");
        String failed = submit("{\"type\":\"flaky\",\"payload\":{\"durationMs\":10,\"failure\":\"permanent\"}}");
        String deadLettered = submit(
                "{\"type\":\"flaky\",\"maxAttempts\":2,\"payload\":{\"durationMs\":10,\"failAttempts\":5}}");
        String cancelled = submit("{\"type\":\"sleep\",\"queue\":\"orphan\"}"); // nobody serves it: stays QUEUED
        assertThat(cancel(cancelled).statusCode()).isEqualTo(200);

        assertThat(awaitTerminal(succeeded).path("state").asText()).isEqualTo("SUCCEEDED");
        assertThat(awaitTerminal(retried).path("state").asText()).isEqualTo("SUCCEEDED");
        assertThat(awaitTerminal(failed).path("state").asText()).isEqualTo("FAILED");
        assertThat(awaitTerminal(deadLettered).path("state").asText()).isEqualTo("DEAD_LETTER");

        // Listeners run right after the state change is stored, so poll until every expectation holds.
        List<String> missing = Await.value("every expected meter in the scrape", WAIT,
                () -> missing(parse(scrape()), List.of(
                        atLeast("taskplatform_jobs_submitted_total", Map.of("queue", "default"), 4),
                        atLeast("taskplatform_jobs_submitted_total", Map.of("queue", "orphan"), 1),
                        present("taskplatform_job_queue_latency_seconds_bucket", Map.of("queue", "default")),
                        atLeast("taskplatform_job_queue_latency_seconds_count", Map.of("queue", "default"), 6),
                        present("taskplatform_job_execution_latency_seconds_bucket", Map.of("outcome", "acked")),
                        present("taskplatform_job_execution_latency_seconds_bucket", Map.of("outcome", "nacked")),
                        atLeast("taskplatform_job_retries_total", Map.of("queue", "default", "reason", "retryable"),
                                2),
                        atLeast("taskplatform_jobs_finished_total", Map.of("outcome", "succeeded"), 2),
                        atLeast("taskplatform_jobs_finished_total", Map.of("outcome", "failed"), 1),
                        atLeast("taskplatform_jobs_finished_total",
                                Map.of("outcome", "dead_letter", "queue", "default"), 1),
                        atLeast("taskplatform_jobs_finished_total", Map.of("outcome", "cancelled", "queue", "orphan"),
                                1),
                        atLeast("taskplatform_job_attempts_total", Map.of("outcome", "acked"), 2),
                        atLeast("taskplatform_job_attempts_total", Map.of("outcome", "nacked"), 4),
                        present("taskplatform_worker_utilization", Map.of("pool", "default", "queue", "default")),
                        present("taskplatform_worker_utilization", Map.of("pool", "slow", "queue", "slow")),
                        new Expectation("taskplatform_worker_busy_time_seconds_total{pool=default} > 0",
                                s -> sum(s, "taskplatform_worker_busy_time_seconds_total",
                                        Map.of("pool", "default")) > 0),
                        atLeast("taskplatform_reconciler_recovered_total", Map.of("kind", "retry_released"), 2))),
                List::isEmpty);
        assertThat(missing).isEmpty();

        List<Sample> samples = parse(scrape());
        assertThat(sum(samples, "taskplatform_jobs_submitted_total", Map.of("queue", "default"))).isEqualTo(4);
        assertThat(samples).filteredOn(s -> s.name().equals("taskplatform_worker_utilization"))
                .extracting(s -> s.labels().get("pool")).contains("default", "slow");
        assertThat(samples).filteredOn(s -> s.name().equals("taskplatform_worker_utilization"))
                .allSatisfy(s -> assertThat(s.value()).isBetween(0.0, 1.0));
    }

    @Test
    void staleLeaseIsReclaimedByTheReconcilerAndCounted() {
        UUID orphan = jobService.submit(new SubmitJobRequest("sleep", "orphan", null, 1, null), null).job().id();
        Optional<Lease> ghost = lifecycle.claim(orphan, "ghost");
        assertThat(ghost).isPresent(); // and it never heartbeats

        JsonNode reclaimed = awaitTerminal(orphan.toString());

        assertThat(reclaimed.path("state").asText()).isEqualTo("DEAD_LETTER");
        assertThat(reclaimed.path("lastError").asText()).contains("LEASE_EXPIRED");
        assertThat(reclaimed.path("deliveries")).hasSize(1);
        assertThat(reclaimed.path("deliveries").get(0).path("ackState").asText()).isEqualTo("EXPIRED");
        assertThat(reclaimed.path("deliveries").get(0).path("workerId").asText()).isEqualTo("ghost");
        List<String> missing = Await.value("stale-lease meters in the scrape", WAIT,
                () -> missing(parse(scrape()), List.of(
                        atLeast("taskplatform_leases_expired_total", Map.of("queue", "orphan"), 1),
                        atLeast("taskplatform_job_attempts_total", Map.of("outcome", "expired", "queue", "orphan"), 1),
                        present("taskplatform_job_execution_latency_seconds_bucket",
                                Map.of("outcome", "expired", "queue", "orphan")),
                        atLeast("taskplatform_jobs_finished_total",
                                Map.of("outcome", "dead_letter", "queue", "orphan"), 1))),
                List::isEmpty);
        assertThat(missing).isEmpty();
    }

    // ------------------------------------------------------------------------------------------ expectations

    private record Expectation(String description, Predicate<List<Sample>> holds) { }

    private static Expectation atLeast(String name, Map<String, String> labels, double min) {
        return new Expectation(name + labels + " >= " + min, s -> sum(s, name, labels) >= min);
    }

    private static Expectation present(String name, Map<String, String> labels) {
        return new Expectation(name + labels + " present", s -> s.stream().anyMatch(x -> matches(x, name, labels)));
    }

    /** Descriptions of the expectations that do not hold for {@code samples}. */
    private static List<String> missing(List<Sample> samples, List<Expectation> expectations) {
        return expectations.stream().filter(e -> !e.holds().test(samples)).map(Expectation::description).toList();
    }

    // ------------------------------------------------------------------------------------------ Prometheus text

    private record Sample(String name, Map<String, String> labels, double value) { }

    private static List<Sample> parse(String text) {
        List<Sample> samples = new ArrayList<>();
        for (String line : text.split("\n")) {
            if (line.isBlank() || line.startsWith("#")) continue;
            Matcher m = SAMPLE.matcher(line.trim());
            if (!m.find()) continue;
            Map<String, String> labels = new LinkedHashMap<>();
            if (m.group(2) != null) {
                Matcher l = LABEL.matcher(m.group(2));
                while (l.find()) labels.put(l.group(1), l.group(2));
            }
            samples.add(new Sample(m.group(1), labels, Double.parseDouble(m.group(3))));
        }
        return samples;
    }

    private static boolean matches(Sample s, String name, Map<String, String> labels) {
        return s.name().equals(name) && labels.entrySet().stream()
                .allMatch(e -> e.getValue().equals(s.labels().get(e.getKey())));
    }

    private static double sum(List<Sample> samples, String name, Map<String, String> labels) {
        return samples.stream().filter(s -> matches(s, name, labels)).mapToDouble(Sample::value).sum();
    }

    // ------------------------------------------------------------------------------------------ HTTP

    private String scrape() {
        HttpResponse<String> r = send(HttpRequest.newBuilder(uri("/actuator/prometheus"))
                .header("Accept", "text/plain").GET());
        assertThat(r.statusCode()).isEqualTo(200);
        assertThat(r.headers().firstValue("Content-Type")).hasValueSatisfying(
                ct -> assertThat(ct).startsWith("text/plain"));
        return r.body();
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

    private String submit(String body) {
        HttpResponse<String> r = send(HttpRequest.newBuilder(uri("/v1/jobs"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)));
        assertThat(r.statusCode()).as(r.body()).isEqualTo(201);
        return json(r.body()).path("id").asText();
    }

    private HttpResponse<String> cancel(String id) {
        return send(HttpRequest.newBuilder(uri("/v1/jobs/" + id + "/cancel"))
                .POST(HttpRequest.BodyPublishers.noBody()));
    }

    private JsonNode awaitTerminal(String id) {
        return Await.value("job " + id + " terminal", WAIT, () -> {
            HttpResponse<String> r = send(HttpRequest.newBuilder(uri("/v1/jobs/" + id)).GET());
            assertThat(r.statusCode()).as(r.body()).isEqualTo(200);
            return json(r.body());
        }, j -> List.of("SUCCEEDED", "FAILED", "DEAD_LETTER", "CANCELLED").contains(j.path("state").asText()));
    }

    private JsonNode json(String body) {
        try {
            return mapper.readTree(body);
        } catch (IOException e) {
            throw new UncheckedIOException("not JSON: " + body, e);
        }
    }
}
