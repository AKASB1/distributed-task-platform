package io.akasb.taskplatform.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.LockSupport;

/**
 * Drives a running service over HTTP with a mixed workload and measures it from the stored job and delivery
 * timestamps. Workload: 70 % short (10-50 ms), 25 % medium (100-300 ms), 5 % long (1-2 s); independently 10 % fail
 * transiently once (then succeed) and 1 % fail permanently. Everything is drawn from one seeded {@link Random}.
 */
public final class LoadGenerator {

    /** {@code ratePerSecond <= 0} submits as fast as possible (burst); otherwise submissions are paced (open loop). */
    public record Params(int jobs, int submitters, long seed, String queue, Duration timeout, int maxAttempts,
                         double ratePerSecond) { }

    /** One generated job: its class, sleep duration and failure mode. */
    public record Spec(String size, long durationMs, String failure, long failSeed) {
        ObjectNode body(ObjectMapper mapper, String queue, int maxAttempts) {
            ObjectNode b = mapper.createObjectNode();
            b.put("queue", queue).put("maxAttempts", maxAttempts).put("timeoutMs", 30_000);
            ObjectNode payload = b.putObject("payload").put("durationMs", durationMs);
            switch (failure) {
                case "none" -> b.put("type", "sleep");
                case "transient" -> {
                    // the first attempt always fails; each later attempt fails with a fixed probability
                    b.put("type", "flaky");
                    payload.put("failAttempts", 1).put("failure", "retryable")
                            .put("failProbability", TRANSIENT_RETRY_FAILURE_PROBABILITY).put("failSeed", failSeed);
                }
                case "permanent" -> {
                    b.put("type", "flaky");
                    payload.put("failAttempts", 1_000).put("failure", "permanent");
                }
                default -> throw new IllegalArgumentException(failure);
            }
            return b;
        }
    }

    /** One attempt as stored by the service. */
    public record Attempt(String jobId, String size, String failure, String finalState, int attempt, String ackState,
                          Instant queuedAt, Instant startedAt, Instant finishedAt) { }

    public record Result(Params params, List<Spec> specs, Instant firstSubmit, Instant lastSubmit, Instant drainedAt,
                         boolean drained, List<JsonNode> jobs, List<Attempt> attempts, String prometheus,
                         List<Long> submitLatencyMicros) { }

    private final URI base;
    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http = HttpClient.newBuilder()
            .proxy(HttpClient.Builder.NO_PROXY)
            .connectTimeout(Duration.ofSeconds(5))
            .executor(Executors.newFixedThreadPool(8))
            .build();

    public LoadGenerator(URI base) {
        this.base = base;
    }

    /** Probability that a retry of a transiently failing job fails again. */
    public static final double TRANSIENT_RETRY_FAILURE_PROBABILITY = 0.3;

    public static List<Spec> workload(int jobs, long seed) {
        Random random = new Random(seed);
        List<Spec> specs = new ArrayList<>(jobs);
        for (int i = 0; i < jobs; i++) {
            double size = random.nextDouble();
            Spec spec;
            String failure;
            double f = random.nextDouble();
            failure = f < 0.01 ? "permanent" : f < 0.11 ? "transient" : "none";
            long failSeed = random.nextLong();
            if (size < 0.70) spec = new Spec("short", 10 + random.nextInt(41), failure, failSeed);
            else if (size < 0.95) spec = new Spec("medium", 100 + random.nextInt(201), failure, failSeed);
            else spec = new Spec("long", 1_000 + random.nextInt(1_001), failure, failSeed);
            specs.add(spec);
        }
        return specs;
    }

    public Result run(Params params) throws Exception {
        List<Spec> specs = workload(params.jobs(), params.seed());
        String[] ids = new String[specs.size()];
        List<Long> submitLatency = Collections.synchronizedList(new ArrayList<>());
        Instant firstSubmit = Instant.now();
        long t0 = System.nanoTime();
        long intervalNanos = params.ratePerSecond() > 0 ? (long) (1e9 / params.ratePerSecond()) : 0;
        try (ExecutorService pool = Executors.newFixedThreadPool(params.submitters())) {
            List<Future<?>> futures = new ArrayList<>();
            for (int s = 0; s < params.submitters(); s++) {
                final int offset = s;
                futures.add(pool.submit(() -> {
                    for (int i = offset; i < specs.size(); i += params.submitters()) {
                        if (intervalNanos > 0) {
                            long due = t0 + i * intervalNanos;
                            for (long wait = due - System.nanoTime(); wait > 0; wait = due - System.nanoTime()) {
                                LockSupport.parkNanos(wait);
                            }
                        }
                        long sent = System.nanoTime();
                        JsonNode created = send("POST", "/v1/jobs",
                                specs.get(i).body(mapper, params.queue(), params.maxAttempts()).toString(), 201);
                        submitLatency.add((System.nanoTime() - sent) / 1_000);
                        ids[i] = created.get("id").asText();
                    }
                    return null;
                }));
            }
            for (Future<?> f : futures) f.get();
        }
        Instant lastSubmit = Instant.now();

        long deadline = System.nanoTime() + params.timeout().toNanos();
        boolean drained = false;
        while (System.nanoTime() < deadline) {
            JsonNode counts = send("GET", "/v1/queues/" + params.queue() + "/stats", null, 200).get("counts");
            long open = counts.path("PENDING").asLong() + counts.path("QUEUED").asLong()
                    + counts.path("RUNNING").asLong() + counts.path("RETRY_WAIT").asLong();
            if (open == 0) {
                drained = true;
                break;
            }
            LockSupport.parkNanos(Duration.ofMillis(200).toNanos());
        }
        Instant drainedAt = Instant.now();

        List<JsonNode> jobs = new ArrayList<>(Collections.nCopies(ids.length, null));
        try (ExecutorService pool = Executors.newFixedThreadPool(8)) {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < ids.length; i++) {
                final int idx = i;
                futures.add(pool.submit(() -> {
                    jobs.set(idx, send("GET", "/v1/jobs/" + ids[idx], null, 200));
                    return null;
                }));
            }
            for (Future<?> f : futures) f.get();
        }
        List<Attempt> attempts = new ArrayList<>();
        for (int i = 0; i < jobs.size(); i++) {
            JsonNode job = jobs.get(i);
            for (JsonNode d : job.path("deliveries")) {
                attempts.add(new Attempt(job.get("id").asText(), specs.get(i).size(), specs.get(i).failure(),
                        job.get("state").asText(), d.get("attempt").asInt(), d.get("ackState").asText(),
                        instant(d, "queuedAt"), instant(d, "startedAt"), instant(d, "finishedAt")));
            }
        }
        String prometheus = http.send(HttpRequest.newBuilder(base.resolve("/actuator/prometheus")).build(),
                HttpResponse.BodyHandlers.ofString()).body();
        return new Result(params, specs, firstSubmit, lastSubmit, drainedAt, drained, jobs, attempts, prometheus,
                List.copyOf(submitLatency));
    }

    private JsonNode send(String method, String path, String body, int expected) throws IOException,
            InterruptedException {
        HttpRequest.Builder b = HttpRequest.newBuilder(base.resolve(path)).timeout(Duration.ofSeconds(30));
        if (body != null) {
            b.header("Content-Type", "application/json").method(method, HttpRequest.BodyPublishers.ofString(body));
        } else {
            b.method(method, HttpRequest.BodyPublishers.noBody());
        }
        HttpResponse<String> r = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() != expected) {
            throw new IllegalStateException(method + " " + path + " -> " + r.statusCode() + ": " + r.body());
        }
        return mapper.readTree(r.body());
    }

    private static Instant instant(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v == null || v.isNull() ? null : Instant.parse(v.asText());
    }
}
