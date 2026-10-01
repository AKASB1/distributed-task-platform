package io.akasb.taskplatform.dispatch;

import java.time.Duration;
import java.util.Objects;

/** A validated submission. {@code payload} is JSON text; {@code idempotencyKey} may be null. */
public record SubmitCommand(String queue, String type, String payload, int maxAttempts, Duration timeout,
                            String idempotencyKey) {
    public SubmitCommand {
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(timeout, "timeout");
    }
}
