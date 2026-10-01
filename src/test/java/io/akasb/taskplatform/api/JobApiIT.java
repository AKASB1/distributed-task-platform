package io.akasb.taskplatform.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.akasb.taskplatform.domain.JobState;
import io.akasb.taskplatform.persistence.JobRepository;
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * The REST API through the real HTTP stack (Tomcat on a random port, embedded PostgreSQL, real worker pools): status
 * codes, RFC 7807 problem bodies, idempotency, cancellation, queue stats and complete job runs.
 *
 * <p>Pools: {@code default} (2 slots) and {@code slow} (1 slot). A {@code flaky} job with {@code failure=hang} on
 * {@code slow} occupies its only slot, so later jobs on that queue stay QUEUED until it is cancelled.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
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
class JobApiIT {
    private static final Duration WAIT = Duration.ofSeconds(20);
    private static final Set<String> TERMINAL = Set.of("SUCCEEDED", "FAILED", "DEAD_LETTER", "CANCELLED");
    private static final String PROBLEM_JSON = "application/problem+json";
    private static final String HANG_ON_SLOW = q("{'type':'flaky','queue':'slow','payload':{'failure':'hang'}}");

    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    /** Jobs that may still hold a worker slot; cancelled after each test so they cannot block the next one. */
    private final List<String> toCancel = new CopyOnWriteArrayList<>();

    @LocalServerPort
    int port;

    @Autowired
    JobRepository repository;

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        PostgresTestProperties.register(registry);
    }

    @AfterEach
    void cancelLeftoversAndCloseClient() {
        try {
            for (String id : toCancel) cancel(id); // 200 or 409 (already finished): both fine
        } finally {
            http.close();
        }
    }

    // ------------------------------------------------------------------------------------------ submission

    @Test
    void submitReturns201WithLocationAndDefaults() {
        HttpResponse<String> r = postJob(q("{'type':'sleep','payload':{'durationMs':10}}"), null);

        assertThat(r.statusCode()).as(r.body()).isEqualTo(201);
        assertThat(r.headers().firstValue("Content-Type")).hasValueSatisfying(
                ct -> assertThat(ct).startsWith("application/json"));
        assertThat(r.headers().firstValue(JobController.IDEMPOTENT_REPLAYED)).isEmpty();
        JsonNode body = json(r);
        String id = body.path("id").asText();
        assertThat(UUID.fromString(id)).isNotNull();
        assertThat(r.headers().firstValue("Location")).hasValueSatisfying(
                location -> assertThat(location).endsWith("/v1/jobs/" + id));
        assertThat(body.path("state").asText()).isIn("QUEUED", "RUNNING", "SUCCEEDED");
        assertThat(body.path("version").asLong()).isGreaterThanOrEqualTo(1);
        assertThat(body.path("maxAttempts").asInt()).isEqualTo(3);
        assertThat(body.path("timeoutMs").asLong()).isEqualTo(30_000);
        assertThat(body.path("queue").asText()).isEqualTo("default");
        assertThat(body.path("type").asText()).isEqualTo("sleep");
        assertThat(body.path("payload").path("durationMs").asInt()).isEqualTo(10);
        assertThat(body.path("createdAt").isTextual()).isTrue();

        URI location = uri("/").resolve(r.headers().firstValue("Location").orElseThrow());
        HttpResponse<String> fetched = send(HttpRequest.newBuilder(location).GET());
        assertThat(fetched.statusCode()).isEqualTo(200);
        assertThat(json(fetched).path("id").asText()).isEqualTo(id);
    }

    @Test
    void replayWithSameKeyAndBodyReturns200WithTheSameJob() {
        String key = "replay-" + UUID.randomUUID();
        String body = q("{'type':'sleep','payload':{'durationMs':10}}");
        long before = totalJobs();

        HttpResponse<String> first = postJob(body, key);
        HttpResponse<String> second = postJob(body, key);

        assertThat(first.statusCode()).as(first.body()).isEqualTo(201);
        assertThat(second.statusCode()).as(second.body()).isEqualTo(200);
        assertThat(second.headers().firstValue(JobController.IDEMPOTENT_REPLAYED)).hasValue("true");
        String id = json(first).path("id").asText();
        assertThat(json(second).path("id").asText()).isEqualTo(id);
        assertThat(json(second).path("idempotencyKey").asText()).isEqualTo(key);
        assertThat(second.headers().firstValue("Location")).hasValueSatisfying(
                location -> assertThat(location).endsWith("/v1/jobs/" + id));
        assertThat(totalJobs()).isEqualTo(before + 1);
    }

    @Test
    void replayIsRecognisedWhenThePayloadHoldsAnIntegralFloatingPointNumber() {
        // Jackson writes 12345678.0 as 1.2345678E7; jsonb hands it back as 12345678. Same request, same key.
        String key = "replay-float-" + UUID.randomUUID();
        String body = q("{'type':'sleep','payload':{'durationMs':10,'amount':12345678.0}}");

        HttpResponse<String> first = postJob(body, key);
        HttpResponse<String> second = postJob(body, key);

        assertThat(first.statusCode()).as(first.body()).isEqualTo(201);
        String id = json(first).path("id").asText();
        assertThat(second.statusCode())
                .as("replay answered %s; submitted payload %s, stored payload %s", second.body(),
                        json(first).path("payload"), getJob(id).path("payload"))
                .isEqualTo(200);
        assertThat(json(second).path("id").asText()).isEqualTo(id);
    }

    @Test
    void sameKeyWithDifferentBodyIs422WithTheExistingJobId() {
        String key = "conflict-" + UUID.randomUUID();
        long before = totalJobs();

        HttpResponse<String> first = postJob(q("{'type':'sleep','payload':{'durationMs':10}}"), key);
        HttpResponse<String> second = postJob(q("{'type':'sleep','payload':{'durationMs':11}}"), key);

        assertThat(first.statusCode()).as(first.body()).isEqualTo(201);
        assertProblem(second, 422);
        JsonNode problem = json(second);
        assertThat(problem.path("title").asText()).isEqualTo("Idempotency key reused");
        assertThat(problem.path("existingJobId").asText()).isEqualTo(json(first).path("id").asText());
        assertThat(problem.path("detail").asText()).contains(key);
        assertThat(totalJobs()).isEqualTo(before + 1);
    }

    @Test
    void concurrentSubmissionsWithOneKeyCreateExactlyOneJob() throws Exception {
        int clients = 8;
        String key = "race-" + UUID.randomUUID();
        String body = q("{'type':'sleep','payload':{'durationMs':10}}");
        long before = totalJobs();
        CyclicBarrier start = new CyclicBarrier(clients);
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        List<HttpResponse<String>> responses = new ArrayList<>();
        try {
            List<Future<HttpResponse<String>>> futures = new ArrayList<>();
            for (int i = 0; i < clients; i++) {
                futures.add(pool.submit(() -> {
                    start.await(10, TimeUnit.SECONDS);
                    return postJob(body, key);
                }));
            }
            for (Future<HttpResponse<String>> f : futures) responses.add(f.get(30, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
            assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(responses).allSatisfy(r -> assertThat(r.statusCode()).as(r.body()).isIn(200, 201));
        assertThat(responses).filteredOn(r -> r.statusCode() == 201).hasSize(1);
        assertThat(responses).filteredOn(r -> r.statusCode() == 200).allSatisfy(r ->
                assertThat(r.headers().firstValue(JobController.IDEMPOTENT_REPLAYED)).hasValue("true"));
        Set<String> ids = new HashSet<>();
        responses.forEach(r -> ids.add(json(r).path("id").asText()));
        assertThat(ids).hasSize(1);
        assertThat(totalJobs()).isEqualTo(before + 1);
    }

    // ------------------------------------------------------------------------------------------ rejections

    static Stream<Arguments> invalidSubmissions() {
        String blob = "x".repeat(70 * 1024);
        return Stream.of(
                Arguments.of("missing type", q("{'payload':{}}"), null, "type"),
                Arguments.of("unknown type", q("{'type':'nope'}"), null, "unknown job type 'nope'"),
                Arguments.of("queue with a space", q("{'type':'sleep','queue':'Bad Queue'}"), null,
                        "queue must match"),
                Arguments.of("unserved queue", q("{'type':'sleep','queue':'nope'}"), null,
                        "no worker pool serves queue 'nope'"),
                Arguments.of("maxAttempts 0", q("{'type':'sleep','maxAttempts':0}"), null, "maxAttempts"),
                Arguments.of("maxAttempts 21", q("{'type':'sleep','maxAttempts':21}"), null,
                        "maxAttempts must be between 1 and 20"),
                Arguments.of("timeoutMs 0", q("{'type':'sleep','timeoutMs':0}"), null, "timeoutMs"),
                Arguments.of("payload is an array", q("{'type':'sleep','payload':[1,2]}"), null,
                        "payload must be a JSON object"),
                Arguments.of("malformed JSON", "{\"type\":\"sleep\",", null, null),
                Arguments.of("payload over 64 KiB", q("{'type':'sleep','payload':{'blob':'" + blob + "'}}"), null,
                        "payload exceeds 65536 bytes"),
                Arguments.of("Idempotency-Key with a space", q("{'type':'sleep'}"), "bad key", "Idempotency-Key"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidSubmissions")
    void invalidSubmissionIs400ProblemAndCreatesNothing(String name, String body, String key, String detail) {
        long before = totalJobs();

        HttpResponse<String> r = postJob(body, key);

        assertProblem(r, 400);
        if (detail != null) assertThat(json(r).path("detail").asText()).contains(detail);
        assertThat(totalJobs()).isEqualTo(before);
    }

    @Test
    void validationErrorsAreListedInTheProblem() {
        HttpResponse<String> r = postJob(q("{'queue':'Bad Queue','maxAttempts':0}"), null);

        assertProblem(r, 400);
        List<String> errors = StreamSupport.stream(json(r).path("errors").spliterator(), false)
                .map(JsonNode::asText).toList();
        assertThat(errors).anySatisfy(e -> assertThat(e).startsWith("type "))
                .anySatisfy(e -> assertThat(e).startsWith("queue "))
                .anySatisfy(e -> assertThat(e).startsWith("maxAttempts "));
    }

    @Test
    void nonJsonContentTypeIs415() {
        HttpResponse<String> r = send(HttpRequest.newBuilder(uri("/v1/jobs"))
                .header("Content-Type", "text/plain")
                .POST(HttpRequest.BodyPublishers.ofString("type=sleep")));

        assertProblem(r, 415);
    }

    @Test
    void unknownJobIs404() {
        UUID id = UUID.randomUUID();

        HttpResponse<String> r = get("/v1/jobs/" + id);

        assertProblem(r, 404);
        assertThat(json(r).path("detail").asText()).contains(id.toString());
        assertProblem(cancel(id.toString()), 404);
    }

    @Test
    void malformedJobIdIs400() {
        HttpResponse<String> r = get("/v1/jobs/not-a-uuid");

        assertProblem(r, 400);
        assertThat(json(r).path("detail").asText()).contains("'id'").contains("not-a-uuid");
    }

    // ------------------------------------------------------------------------------------------ cancellation

    @Test
    void cancelsQueuedAndRunningJobs() {
        String hanging = submitTracked(HANG_ON_SLOW);
        awaitState(hanging, "RUNNING");
        String waiting = submitTracked(q("{'type':'sleep','queue':'slow','payload':{'durationMs':10}}"));
        assertThat(getJob(waiting).path("state").asText()).isEqualTo("QUEUED");

        HttpResponse<String> cancelledQueued = cancel(waiting);
        assertThat(cancelledQueued.statusCode()).as(cancelledQueued.body()).isEqualTo(200);
        assertThat(json(cancelledQueued).path("state").asText()).isEqualTo("CANCELLED");
        assertThat(json(cancelledQueued).path("finishedAt").isTextual()).isTrue();
        assertThat(json(cancelledQueued).path("deliveries")).isEmpty();

        long runningVersion = getJob(hanging).path("version").asLong();
        HttpResponse<String> cancelledRunning = cancel(hanging);
        assertThat(cancelledRunning.statusCode()).as(cancelledRunning.body()).isEqualTo(200);
        assertThat(json(cancelledRunning).path("state").asText()).isEqualTo("CANCELLED");
        assertThat(json(cancelledRunning).path("version").asLong()).isEqualTo(runningVersion + 1);

        // The worker learns about the cancel on its next heartbeat and closes the attempt.
        JsonNode afterAck = Await.value("running attempt acknowledged as CANCELLED", WAIT, () -> getJob(hanging),
                j -> j.path("deliveries").size() == 1
                        && "CANCELLED".equals(j.path("deliveries").get(0).path("ackState").asText()));
        assertThat(afterAck.path("state").asText()).isEqualTo("CANCELLED");
        assertThat(afterAck.path("attempts").asInt()).isEqualTo(1);

        // A job queued behind the cancelled ones runs, so the slot has moved past both of them.
        String sentinel = submitTracked(q("{'type':'sleep','queue':'slow','payload':{'durationMs':10}}"));
        assertThat(awaitTerminal(sentinel).path("state").asText()).isEqualTo("SUCCEEDED");
        JsonNode hangingFinal = getJob(hanging);
        assertThat(hangingFinal.path("state").asText()).isEqualTo("CANCELLED");
        assertThat(hangingFinal.path("deliveries")).hasSize(1);
        JsonNode waitingFinal = getJob(waiting);
        assertThat(waitingFinal.path("state").asText()).isEqualTo("CANCELLED");
        assertThat(waitingFinal.path("deliveries")).isEmpty();
    }

    @Test
    void cancellingASucceededJobIs409WithItsState() {
        String id = submit(q("{'type':'sleep','payload':{'durationMs':10}}"));
        assertThat(awaitTerminal(id).path("state").asText()).isEqualTo("SUCCEEDED");

        HttpResponse<String> r = cancel(id);

        assertProblem(r, 409);
        assertThat(json(r).path("state").asText()).isEqualTo("SUCCEEDED");
        assertThat(json(r).path("title").asText()).isEqualTo("Job state conflict");
        assertThat(getJob(id).path("state").asText()).isEqualTo("SUCCEEDED");
    }

    @Test
    void cancellingTwiceReturns200BothTimesAndChangesNothingTheSecondTime() {
        String id = submitTracked(q("{'type':'flaky','payload':{'failure':'hang'}}"));

        HttpResponse<String> first = cancel(id);
        HttpResponse<String> second = cancel(id);

        assertThat(first.statusCode()).as(first.body()).isEqualTo(200);
        assertThat(second.statusCode()).as(second.body()).isEqualTo(200);
        assertThat(json(first).path("state").asText()).isEqualTo("CANCELLED");
        assertThat(json(second).path("state").asText()).isEqualTo("CANCELLED");
        assertThat(json(second).path("version").asLong()).isEqualTo(json(first).path("version").asLong());
        assertThat(json(second).path("finishedAt").asText()).isEqualTo(json(first).path("finishedAt").asText());
        // If a slot had already leased it, that attempt ends as CANCELLED, never as a success.
        JsonNode settled = Await.value("no open delivery", WAIT, () -> getJob(id),
                j -> StreamSupport.stream(j.path("deliveries").spliterator(), false)
                        .noneMatch(d -> "LEASED".equals(d.path("ackState").asText())));
        assertThat(settled.path("state").asText()).isEqualTo("CANCELLED");
        assertThat(settled.path("deliveries")).allSatisfy(
                d -> assertThat(d.path("ackState").asText()).isEqualTo("CANCELLED"));
    }

    // ------------------------------------------------------------------------------------------ end to end

    @Test
    void sleepJobSucceedsWithItsResult() {
        String id = submit(q("{'type':'sleep','payload':{'durationMs':20}}"));

        JsonNode done = awaitTerminal(id);

        assertThat(done.path("state").asText()).isEqualTo("SUCCEEDED");
        assertThat(done.path("result").path("sleptMs").asLong()).isEqualTo(20);
        assertThat(done.path("result").path("attempt").asInt()).isEqualTo(1);
        assertThat(done.path("attempts").asInt()).isEqualTo(1);
        assertThat(done.path("lastError").isMissingNode()).isTrue();
        assertThat(done.path("finishedAt").isTextual()).isTrue();
        assertThat(ackStates(done)).containsExactly("ACKED");
        assertThat(done.path("deliveries").get(0).path("progress").asInt()).isEqualTo(100);
        assertThat(done.path("deliveries").get(0).path("workerId").asText()).startsWith("default@");
    }

    @Test
    void flakyJobSucceedsOnItsSecondAttempt() {
        String id = submit(q("{'type':'flaky','payload':{'durationMs':10,'failAttempts':1}}"));

        JsonNode done = awaitTerminal(id);

        assertThat(done.path("state").asText()).isEqualTo("SUCCEEDED");
        assertThat(done.path("attempts").asInt()).isEqualTo(2);
        assertThat(ackStates(done)).containsExactly("NACKED", "ACKED");
        assertThat(done.path("deliveries").get(0).path("error").asText()).startsWith("RETRYABLE");
        assertThat(done.path("result").path("succeededOnAttempt").asInt()).isEqualTo(2);
    }

    @Test
    void permanentFailureEndsFailedAfterOneAttempt() {
        String id = submit(q("{'type':'flaky','payload':{'durationMs':10,'failure':'permanent'}}"));

        JsonNode done = awaitTerminal(id);

        assertThat(done.path("state").asText()).isEqualTo("FAILED");
        assertThat(done.path("attempts").asInt()).isEqualTo(1);
        assertThat(ackStates(done)).containsExactly("NACKED");
        assertThat(done.path("lastError").asText()).startsWith("NON_RETRYABLE");
        assertThat(done.path("result").isMissingNode()).isTrue();
    }

    @Test
    void exhaustedAttemptsEndInDeadLetter() {
        String id = submit(q("{'type':'flaky','maxAttempts':2,'payload':{'durationMs':10,'failAttempts':5}}"));

        JsonNode done = awaitTerminal(id);

        assertThat(done.path("state").asText()).isEqualTo("DEAD_LETTER");
        assertThat(done.path("attempts").asInt()).isEqualTo(2);
        assertThat(done.path("maxAttempts").asInt()).isEqualTo(2);
        assertThat(ackStates(done)).containsExactly("NACKED", "NACKED");
        assertThat(done.path("lastError").asText()).contains("attempts exhausted (2/2)").contains("RETRYABLE");
    }

    @Test
    void executionTimeoutWithoutAttemptsLeftEndsInDeadLetter() {
        String id = submitTracked(q("{'type':'flaky','maxAttempts':1,'timeoutMs':100,'payload':{'failure':'hang'}}"));

        JsonNode done = awaitTerminal(id);

        assertThat(done.path("state").asText()).isEqualTo("DEAD_LETTER");
        assertThat(done.path("timeoutMs").asLong()).isEqualTo(100);
        assertThat(done.path("lastError").asText()).contains("TIMEOUT");
        assertThat(ackStates(done)).containsExactly("NACKED");
        assertThat(done.path("deliveries").get(0).path("error").asText()).startsWith("TIMEOUT");
    }

    // ------------------------------------------------------------------------------------------ queue stats

    @Test
    void queueStatsCountEveryState() {
        String id = submit(q("{'type':'sleep','payload':{'durationMs':10}}"));
        awaitTerminal(id);

        HttpResponse<String> r = get("/v1/queues/default/stats");

        assertThat(r.statusCode()).as(r.body()).isEqualTo(200);
        JsonNode stats = json(r);
        assertThat(stats.path("queue").asText()).isEqualTo("default");
        List<String> states = new ArrayList<>();
        stats.path("counts").fieldNames().forEachRemaining(states::add);
        assertThat(states).containsExactlyInAnyOrderElementsOf(
                Arrays.stream(JobState.values()).map(Enum::name).toList());
        long sum = 0;
        for (String s : states) sum += stats.path("counts").path(s).asLong();
        assertThat(stats.path("total").asLong()).isEqualTo(sum);
        assertThat(stats.path("counts").path("SUCCEEDED").asLong()).isGreaterThanOrEqualTo(1);
        assertThat(stats.path("oldestQueuedAgeMs").asLong()).isGreaterThanOrEqualTo(0);
        assertThat(stats.path("asOf").isTextual()).isTrue();
    }

    @Test
    void queueStatsShowJobsWaitingBehindAHangingOne() {
        String hanging = submitTracked(HANG_ON_SLOW);
        awaitState(hanging, "RUNNING");
        submitTracked(q("{'type':'sleep','queue':'slow','payload':{'durationMs':10}}"));
        submitTracked(q("{'type':'sleep','queue':'slow','payload':{'durationMs':10}}"));

        HttpResponse<String> r = get("/v1/queues/slow/stats");

        assertThat(r.statusCode()).as(r.body()).isEqualTo(200);
        JsonNode stats = json(r);
        assertThat(stats.path("queue").asText()).isEqualTo("slow");
        assertThat(stats.path("counts").path("RUNNING").asLong()).isGreaterThanOrEqualTo(1);
        assertThat(stats.path("counts").path("QUEUED").asLong()).isGreaterThan(0);
        assertThat(stats.path("oldestQueuedAgeMs").asLong()).isGreaterThanOrEqualTo(0);
        assertThat(stats.path("oldestQueuedAt").isTextual()).isTrue();
    }

    @Test
    void queueStatsRejectAnInvalidQueueName() {
        assertProblem(get("/v1/queues/INVALID/stats"), 400);
        assertProblem(get("/v1/queues/Bad%20Queue/stats"), 400);
    }

    // ------------------------------------------------------------------------------------------ helpers

    /** JSON with single quotes for readability. */
    private static String q(String singleQuoted) {
        return singleQuoted.replace('\'', '"');
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

    private HttpResponse<String> postJob(String body, String idempotencyKey) {
        HttpRequest.Builder b = HttpRequest.newBuilder(uri("/v1/jobs"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (idempotencyKey != null) b.header(JobController.IDEMPOTENCY_KEY, idempotencyKey);
        return send(b);
    }

    private String submit(String body) {
        HttpResponse<String> r = postJob(body, null);
        assertThat(r.statusCode()).as(r.body()).isEqualTo(201);
        return json(r).path("id").asText();
    }

    private String submitTracked(String body) {
        String id = submit(body);
        toCancel.add(id);
        return id;
    }

    private HttpResponse<String> get(String path) {
        return send(HttpRequest.newBuilder(uri(path)).GET());
    }

    private HttpResponse<String> cancel(String id) {
        return send(HttpRequest.newBuilder(uri("/v1/jobs/" + id + "/cancel"))
                .POST(HttpRequest.BodyPublishers.noBody()));
    }

    private JsonNode getJob(String id) {
        HttpResponse<String> r = get("/v1/jobs/" + id);
        assertThat(r.statusCode()).as(r.body()).isEqualTo(200);
        return json(r);
    }

    private JsonNode awaitTerminal(String id) {
        return Await.value("job " + id + " terminal", WAIT, () -> getJob(id),
                j -> TERMINAL.contains(j.path("state").asText()));
    }

    private JsonNode awaitState(String id, String state) {
        return Await.value("job " + id + " " + state, WAIT, () -> getJob(id),
                j -> state.equals(j.path("state").asText()));
    }

    private JsonNode json(HttpResponse<String> r) {
        try {
            return mapper.readTree(r.body());
        } catch (IOException e) {
            throw new UncheckedIOException("not JSON: " + r.body(), e);
        }
    }

    private static List<String> ackStates(JsonNode job) {
        return StreamSupport.stream(job.path("deliveries").spliterator(), false)
                .map(d -> d.path("ackState").asText()).toList();
    }

    private void assertProblem(HttpResponse<String> r, int status) {
        assertThat(r.statusCode()).as(r.body()).isEqualTo(status);
        assertThat(r.headers().firstValue("Content-Type")).as("Content-Type of " + r.body())
                .hasValueSatisfying(ct -> assertThat(ct).startsWith(PROBLEM_JSON));
        JsonNode problem = json(r);
        assertThat(problem.path("status").asInt()).isEqualTo(status);
        assertThat(problem.path("title").asText()).isNotBlank();
        assertThat(problem.path("detail").asText()).isNotBlank();
    }

    private long totalJobs() {
        return repository.countByState().values().stream().mapToLong(Long::longValue).sum();
    }
}
