package io.akasb.taskplatform.worker.handlers;

import io.akasb.taskplatform.worker.TaskContext;
import io.akasb.taskplatform.worker.TaskHandler;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Sample handler that always succeeds: sleeps {@code durationMs} (default 100) in up to 10 steps, reporting progress
 * after each step. Payload: {@code {"durationMs": 250}}.
 */
public final class SleepTaskHandler implements TaskHandler {
    public static final String TYPE = "sleep";
    static final long MAX_DURATION_MS = 3_600_000;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Object handle(TaskContext context) throws InterruptedException {
        long duration = Math.min(MAX_DURATION_MS, Math.max(0, context.payload().path("durationMs").asLong(100)));
        work(context, duration);
        return Map.of("sleptMs", duration, "attempt", context.attempt());
    }

    /**
     * Sleeps {@code durationMs} in up to 10 steps, reporting progress after each; stops with InterruptedException when
     * cancelled. Each step sleeps until its deadline measured from the start, so coarse OS timer granularity (about
     * 15.6 ms on Windows) does not add up across steps.
     */
    static void work(TaskContext context, long durationMs) throws InterruptedException {
        int steps = (int) Math.max(1, Math.min(10, durationMs / 10));
        long start = System.nanoTime();
        long total = TimeUnit.MILLISECONDS.toNanos(durationMs);
        for (int i = 1; i <= steps; i++) {
            context.checkCancelled();
            long stepEnd = start + total * i / steps;
            for (long remaining = stepEnd - System.nanoTime(); remaining > 0; remaining = stepEnd - System.nanoTime()) {
                TimeUnit.NANOSECONDS.sleep(remaining);
            }
            context.reportProgress(i * 100 / steps);
        }
    }
}
