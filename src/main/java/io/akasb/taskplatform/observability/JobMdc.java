package io.akasb.taskplatform.observability;

import java.util.UUID;
import org.slf4j.MDC;

/**
 * Puts job context into the SLF4J MDC so structured log lines carry {@code jobId}, {@code attempt}, {@code queue} and
 * {@code workerId}. Use with try-with-resources; closing restores the previous values.
 */
public final class JobMdc implements AutoCloseable {
    public static final String JOB_ID = "jobId";
    public static final String ATTEMPT = "attempt";
    public static final String QUEUE = "queue";
    public static final String WORKER_ID = "workerId";

    private final String[] keys = {JOB_ID, ATTEMPT, QUEUE, WORKER_ID};
    private final String[] previous = new String[keys.length];

    private JobMdc(UUID jobId, Integer attempt, String queue, String workerId) {
        String[] values = {
                jobId == null ? null : jobId.toString(),
                attempt == null ? null : attempt.toString(),
                queue,
                workerId};
        for (int i = 0; i < keys.length; i++) {
            previous[i] = MDC.get(keys[i]);
            if (values[i] != null) MDC.put(keys[i], values[i]);
        }
    }

    public static JobMdc of(UUID jobId, Integer attempt, String queue, String workerId) {
        return new JobMdc(jobId, attempt, queue, workerId);
    }

    public static JobMdc of(UUID jobId, String queue) {
        return new JobMdc(jobId, null, queue, null);
    }

    @Override
    public void close() {
        for (int i = 0; i < keys.length; i++) {
            if (previous[i] == null) MDC.remove(keys[i]);
            else MDC.put(keys[i], previous[i]);
        }
    }
}
