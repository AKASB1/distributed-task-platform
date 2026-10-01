package io.akasb.taskplatform.worker.handlers;

import io.akasb.taskplatform.worker.NonRetryableTaskException;
import io.akasb.taskplatform.worker.RetryableTaskException;
import io.akasb.taskplatform.worker.TaskContext;
import io.akasb.taskplatform.worker.TaskHandler;
import java.util.Map;
import java.util.SplittableRandom;

/**
 * Sample handler that fails in a configurable, deterministic way. It works for {@code durationMs} (default 50), then
 * fails while {@code attempt <= failAttempts} and succeeds afterwards.
 *
 * <p>Payload fields:
 * <ul>
 *   <li>{@code failAttempts} (default 1): how many leading attempts fail;</li>
 *   <li>{@code failure} (default {@code "retryable"}): {@code "retryable"} throws a retryable error,
 *       {@code "permanent"} a non-retryable one, {@code "hang"} blocks until interrupted (to exercise timeouts and
 *       cancellation of running jobs);</li>
 *   <li>{@code failProbability} (default 0) and {@code failSeed} (default 0): each attempt after the first
 *       {@code failAttempts} also fails with this probability, decided deterministically from the seed and the
 *       attempt number (so a load test is reproducible).</li>
 * </ul>
 * Example: {@code {"durationMs": 20, "failAttempts": 2, "failure": "retryable"}}.
 */
public final class FlakyTaskHandler implements TaskHandler {
    public static final String TYPE = "flaky";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Object handle(TaskContext context) throws InterruptedException {
        long duration = Math.min(SleepTaskHandler.MAX_DURATION_MS,
                Math.max(0, context.payload().path("durationMs").asLong(50)));
        int failAttempts = context.payload().path("failAttempts").asInt(1);
        String failure = context.payload().path("failure").asText("retryable");
        double failProbability = context.payload().path("failProbability").asDouble(0.0);
        long failSeed = context.payload().path("failSeed").asLong(0L);
        if (context.attempt() <= failAttempts && failure.equals("hang")) {
            while (true) {
                context.checkCancelled();
                Thread.sleep(50);
            }
        }
        SleepTaskHandler.work(context, duration);
        boolean leading = context.attempt() <= failAttempts;
        boolean random = !leading && failProbability > 0
                && new SplittableRandom(failSeed * 31 + context.attempt()).nextDouble() < failProbability;
        if (leading || random) {
            String message = leading
                    ? "injected failure on attempt " + context.attempt() + " of " + failAttempts
                    : "injected random failure on attempt " + context.attempt() + " (p=" + failProbability + ")";
            if (failure.equals("permanent")) throw new NonRetryableTaskException(message);
            throw new RetryableTaskException(message);
        }
        return Map.of("succeededOnAttempt", context.attempt());
    }
}
