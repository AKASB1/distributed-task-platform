package io.akasb.taskplatform.dispatch;

import io.akasb.taskplatform.domain.Job;

/** {@code created = false} means the idempotency key was already used and {@code job} is the existing job. */
public record SubmitResult(Job job, boolean created) {
}
