package io.akasb.taskplatform.bench;

import static org.assertj.core.api.Assertions.assertThat;

import io.akasb.taskplatform.Application;
import io.akasb.taskplatform.support.EmbeddedPostgresSupport;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Small load test (excluded from the normal build). Starts the service in-process on embedded PostgreSQL, or targets
 * a running instance, submits the mixed workload over HTTP and writes the results under {@code benchmarks/results/}.
 *
 * <pre>
 * ./mvnw -B -Pload test                                   (2,000 jobs, 8 workers)
 * ./mvnw -B -Pload test -Dload.jobs=500 -Dload.workers=4
 * ./mvnw -B -Pload test -Dload.ratePerSecond=40            (paced submission instead of a burst)
 * ./mvnw -B -Pload test -Dload.baseUrl=http://127.0.0.1:18080 -Dload.queue=default   (external service)
 * </pre>
 */
@Tag("load")
class LoadTest {

    @Test
    void mixedWorkload() throws Exception {
        int jobs = Integer.getInteger("load.jobs", 2_000);
        int workers = Integer.getInteger("load.workers", 8);
        long seed = Long.getLong("load.seed", 42L);
        int submitters = Integer.getInteger("load.submitters", 8);
        int maxAttempts = Integer.getInteger("load.maxAttempts", 3);
        Duration timeout = Duration.ofSeconds(Integer.getInteger("load.timeoutSeconds", 270));
        double rate = Double.parseDouble(System.getProperty("load.ratePerSecond", "0"));
        String baseUrl = System.getProperty("load.baseUrl", "");
        String queue = System.getProperty("load.queue", "bench");
        Path output = Path.of(System.getProperty("load.output", "benchmarks/results"));

        Map<String, String> config = new LinkedHashMap<>();
        config.put("jobs", String.valueOf(jobs));
        config.put("queue", queue);
        config.put("submitter threads", String.valueOf(submitters));
        config.put("maxAttempts per job", String.valueOf(maxAttempts));
        config.put("workload seed", String.valueOf(seed));
        config.put("submission", rate > 0 ? "paced at " + rate + " jobs/s (open loop)" : "burst (as fast as possible)");
        config.put("transient jobs", "first attempt fails, later attempts fail with p="
                + LoadGenerator.TRANSIENT_RETRY_FAILURE_PROBABILITY);
        ConfigurableApplicationContext context = null;
        try {
            URI base;
            if (baseUrl.isBlank()) {
                Map<String, Object> props = new LinkedHashMap<>();
                props.put("server.port", "0");
                props.put("server.address", "127.0.0.1");
                props.put("spring.datasource.url", EmbeddedPostgresSupport.newDatabase(false));
                props.put("spring.datasource.username", EmbeddedPostgresSupport.USER);
                props.put("spring.datasource.password", "");
                props.put("spring.datasource.hikari.maximum-pool-size", String.valueOf(Math.max(20, workers + 12)));
                props.put("taskplatform.workers.pools[0].name", "bench");
                props.put("taskplatform.workers.pools[0].queue", queue);
                props.put("taskplatform.workers.pools[0].concurrency", String.valueOf(workers));
                props.put("taskplatform.reconciler.interval", "100ms");
                props.put("taskplatform.retry.seed", String.valueOf(seed));
                props.put("logging.level.io.akasb.taskplatform", "WARN");
                // command-line arguments outrank application.yml (builder "properties" would only be defaults)
                String[] args = props.entrySet().stream().map(e -> "--" + e.getKey() + "=" + e.getValue())
                        .toArray(String[]::new);
                context = new SpringApplicationBuilder(Application.class).run(args);
                base = URI.create("http://127.0.0.1:" + context.getEnvironment().getProperty("local.server.port"));
                config.put("service", "in-process (same JVM as the load generator), embedded PostgreSQL 17 on the same machine");
                config.put("worker pool", "1 pool, " + workers + " slots");
                config.put("lease / heartbeat", "30 s / 10 s (defaults)");
                config.put("retry policy", "initial 1 s, x2, cap 60 s, jitter 0.5, seed " + seed + " (defaults + seed)");
                config.put("reconciler interval", "100 ms");
                config.put("dispatcher", "in-memory");
            } else {
                base = URI.create(baseUrl);
                config.put("service", "external at " + baseUrl);
            }
            LoadGenerator.Result result = new LoadGenerator(base).run(
                    new LoadGenerator.Params(jobs, submitters, seed, queue, timeout, maxAttempts, rate));
            LoadReport report = new LoadReport(result, config);
            String stamp = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").format(LocalDateTime.now());
            String mode = rate > 0 ? "-paced" + Math.round(rate) : "-burst";
            Path dir = report.write(output.resolve(stamp + "-" + jobs + "jobs-" + workers + "workers" + mode));
            System.out.println(report.summary());
            System.out.println("raw results written to " + dir.toAbsolutePath());

            assertThat(result.drained()).as("all jobs reached a terminal state before the timeout").isTrue();
            assertThat(report.duplicateCompletions()).as("no job acknowledged twice").isZero();
            assertThat(report.finalStates().keySet()).allMatch(s -> s.equals("SUCCEEDED") || s.equals("FAILED")
                    || s.equals("DEAD_LETTER"));
        } finally {
            if (context != null) context.close();
        }
    }
}
