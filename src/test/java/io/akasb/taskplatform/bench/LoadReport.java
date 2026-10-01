package io.akasb.taskplatform.bench;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Turns a {@link LoadGenerator.Result} into a summary and raw files. */
public final class LoadReport {
    private final LoadGenerator.Result result;
    private final Map<String, String> config;

    public LoadReport(LoadGenerator.Result result, Map<String, String> config) {
        this.result = result;
        this.config = config;
    }

    public Map<String, Long> finalStates() {
        return result.jobs().stream().collect(Collectors.groupingBy(j -> j.get("state").asText(), TreeMap::new,
                Collectors.counting()));
    }

    public long retries() {
        return result.attempts().size() - result.jobs().stream().filter(j -> j.path("deliveries").size() > 0).count();
    }

    /** Jobs that were acknowledged more than once (must be zero). */
    public long duplicateCompletions() {
        return result.attempts().stream().filter(a -> a.ackState().equals("ACKED"))
                .collect(Collectors.groupingBy(LoadGenerator.Attempt::jobId, Collectors.counting()))
                .values().stream().filter(n -> n > 1).count();
    }

    public Instant lastFinish() {
        return result.jobs().stream().map(j -> j.path("finishedAt").asText(null)).filter(s -> s != null)
                .map(Instant::parse).max(Comparator.naturalOrder()).orElse(result.drainedAt());
    }

    public String summary() {
        Duration makespan = Duration.between(result.firstSubmit(), lastFinish());
        double seconds = makespan.toNanos() / 1e9;
        Map<String, Long> states = finalStates();
        long terminal = states.entrySet().stream().filter(e -> !List.of("PENDING", "QUEUED", "RUNNING",
                "RETRY_WAIT").contains(e.getKey())).mapToLong(Map.Entry::getValue).sum();
        List<Double> queue = millis(result.attempts(), true);
        List<Double> exec = millis(result.attempts(), false);
        List<Double> queueFirst = millis(result.attempts().stream().filter(a -> a.attempt() == 1).toList(), true);
        StringBuilder s = new StringBuilder();
        s.append("# Load test result\n\n");
        s.append("## Environment\n\n");
        environment().forEach((k, v) -> s.append("- ").append(k).append(": ").append(v).append('\n'));
        s.append("\n## Configuration\n\n");
        config.forEach((k, v) -> s.append("- ").append(k).append(": ").append(v).append('\n'));
        s.append("\n## Workload (generated)\n\n");
        Map<String, Long> sizes = result.specs().stream().collect(Collectors.groupingBy(LoadGenerator.Spec::size,
                TreeMap::new, Collectors.counting()));
        Map<String, Long> failures = result.specs().stream().collect(Collectors.groupingBy(LoadGenerator.Spec::failure,
                TreeMap::new, Collectors.counting()));
        s.append("- sizes: ").append(sizes).append('\n');
        s.append("- failure modes: ").append(failures).append('\n');
        s.append("\n## Results\n\n");
        s.append("| Metric | Value |\n|---|---|\n");
        row(s, "jobs submitted", String.valueOf(result.jobs().size()));
        row(s, "drained before timeout", String.valueOf(result.drained()));
        row(s, "final states", states.toString());
        row(s, "makespan (first submit to last finish)", fmt(seconds) + " s");
        row(s, "submission phase", fmt(Duration.between(result.firstSubmit(), result.lastSubmit()).toMillis() / 1e3) + " s");
        row(s, "throughput (terminal jobs / makespan)", fmt(terminal / seconds) + " jobs/s");
        row(s, "throughput (succeeded jobs / makespan)", fmt(states.getOrDefault("SUCCEEDED", 0L) / seconds) + " jobs/s");
        row(s, "attempts", String.valueOf(result.attempts().size()));
        row(s, "retries (attempts - jobs)", String.valueOf(retries()));
        row(s, "dead-lettered jobs", String.valueOf(states.getOrDefault("DEAD_LETTER", 0L)));
        row(s, "failed (non-retryable) jobs", String.valueOf(states.getOrDefault("FAILED", 0L)));
        row(s, "jobs acknowledged twice", String.valueOf(duplicateCompletions()));
        row(s, "queue latency P50 / P95 / P99 (all attempts)", pct(queue));
        row(s, "queue latency P50 / P95 / P99 (first attempts)", pct(queueFirst));
        row(s, "execution latency P50 / P95 / P99 (all attempts)", pct(exec));
        for (String size : List.of("short", "medium", "long")) {
            List<Double> e = millis(result.attempts().stream().filter(a -> a.size().equals(size)).toList(), false);
            row(s, "execution latency P50 / P95 / P99 (" + size + ")", pct(e));
        }
        List<Double> submit = result.submitLatencyMicros().stream().map(us -> us / 1000.0).sorted().toList();
        row(s, "submit request latency P50 / P95 / P99", pct(submit));
        s.append("\nQueue latency = delivery.startedAt - delivery.queuedAt (time from becoming eligible to being leased;"
                + " for retries it starts when the backoff elapsed). Execution latency = delivery.finishedAt -"
                + " delivery.startedAt (lease to acknowledgement). Percentiles use the nearest-rank method.\n");
        return s.toString();
    }

    public Path write(Path directory) throws IOException {
        Files.createDirectories(directory);
        Files.writeString(directory.resolve("summary.md"), summary(), StandardCharsets.UTF_8);
        Files.writeString(directory.resolve("prometheus.txt"), result.prometheus().lines()
                .filter(l -> l.contains("taskplatform_")).collect(Collectors.joining("\n")) + "\n", StandardCharsets.UTF_8);
        StringBuilder csv = new StringBuilder("job_id,size,failure_mode,final_state,attempt,ack_state,queued_at,"
                + "started_at,finished_at,queue_ms,exec_ms\n");
        for (LoadGenerator.Attempt a : result.attempts()) {
            csv.append(a.jobId()).append(',').append(a.size()).append(',').append(a.failure()).append(',')
                    .append(a.finalState()).append(',').append(a.attempt()).append(',').append(a.ackState()).append(',')
                    .append(a.queuedAt()).append(',').append(a.startedAt()).append(',').append(a.finishedAt())
                    .append(',').append(fmt(ms(a.queuedAt(), a.startedAt()))).append(',')
                    .append(fmt(ms(a.startedAt(), a.finishedAt()))).append('\n');
        }
        Files.writeString(directory.resolve("attempts.csv"), csv.toString(), StandardCharsets.UTF_8);
        return directory;
    }

    static Map<String, String> environment() {
        Map<String, String> env = new LinkedHashMap<>();
        env.put("CPU", cpuModel());
        env.put("logical processors", String.valueOf(Runtime.getRuntime().availableProcessors()));
        if (ManagementFactory.getOperatingSystemMXBean() instanceof com.sun.management.OperatingSystemMXBean os) {
            env.put("RAM", Math.round(os.getTotalMemorySize() / (1024.0 * 1024 * 1024)) + " GB");
        }
        env.put("OS", System.getProperty("os.name") + " " + System.getProperty("os.version") + " ("
                + System.getProperty("os.arch") + ")");
        env.put("Java", System.getProperty("java.vendor") + " " + System.getProperty("java.runtime.version"));
        env.put("JVM max heap", Runtime.getRuntime().maxMemory() / (1024 * 1024) + " MB");
        return env;
    }

    private static String cpuModel() {
        String os = System.getProperty("os.name").toLowerCase(Locale.ROOT);
        try {
            if (os.contains("win")) {
                String out = exec("reg", "query", "HKLM\\HARDWARE\\DESCRIPTION\\System\\CentralProcessor\\0", "/v",
                        "ProcessorNameString");
                int i = out.indexOf("REG_SZ");
                if (i >= 0) return out.substring(i + 6).trim();
            } else if (os.contains("mac")) {
                return exec("sysctl", "-n", "machdep.cpu.brand_string").trim();
            } else {
                for (String line : Files.readAllLines(Path.of("/proc/cpuinfo"))) {
                    if (line.startsWith("model name")) return line.substring(line.indexOf(':') + 1).trim();
                }
            }
        } catch (IOException | InterruptedException e) {
            // fall through
        }
        String id = System.getenv("PROCESSOR_IDENTIFIER");
        return id != null ? id : "unknown";
    }

    private static String exec(String... command) throws IOException, InterruptedException {
        Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        p.waitFor(10, TimeUnit.SECONDS);
        return out;
    }

    private static List<Double> millis(List<LoadGenerator.Attempt> attempts, boolean queue) {
        List<Double> values = new ArrayList<>();
        for (LoadGenerator.Attempt a : attempts) {
            double v = queue ? ms(a.queuedAt(), a.startedAt()) : ms(a.startedAt(), a.finishedAt());
            if (!Double.isNaN(v)) values.add(v);
        }
        values.sort(Comparator.naturalOrder());
        return values;
    }

    private static double ms(Instant from, Instant to) {
        if (from == null || to == null) return Double.NaN;
        return Duration.between(from, to).toNanos() / 1e6;
    }

    static double percentile(List<Double> sorted, double p) {
        if (sorted.isEmpty()) return Double.NaN;
        int rank = (int) Math.ceil(p / 100.0 * sorted.size());
        return sorted.get(Math.max(0, Math.min(sorted.size() - 1, rank - 1)));
    }

    private static String pct(List<Double> sorted) {
        return fmt(percentile(sorted, 50)) + " / " + fmt(percentile(sorted, 95)) + " / "
                + fmt(percentile(sorted, 99)) + " ms (n=" + sorted.size() + ")";
    }

    private static void row(StringBuilder s, String k, String v) {
        s.append("| ").append(k).append(" | ").append(v).append(" |\n");
    }

    private static String fmt(double v) {
        return Double.isNaN(v) ? "n/a" : String.format(Locale.ROOT, "%.1f", v);
    }
}
