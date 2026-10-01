package io.akasb.taskplatform.domain;

import java.util.Objects;

public record TaskFailure(FailureKind kind, String message) {
    private static final int MAX_MESSAGE = 2000;

    public TaskFailure {
        Objects.requireNonNull(kind, "kind");
        // PostgreSQL text columns cannot store U+0000; a handler message containing it would make the report fail
        message = message == null ? "" : message.replace('\u0000', '\uFFFD');
        if (message.length() > MAX_MESSAGE) message = message.substring(0, MAX_MESSAGE);
    }

    /** Text stored in {@code last_error} / {@code deliveries.error}. */
    public String describe() {
        return message.isEmpty() ? kind.name() : kind.name() + ": " + message;
    }
}
