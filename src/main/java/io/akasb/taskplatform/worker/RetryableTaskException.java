package io.akasb.taskplatform.worker;

/** Explicitly retryable failure. Any other exception except {@link NonRetryableTaskException} is retryable too. */
public class RetryableTaskException extends RuntimeException {
    public RetryableTaskException(String message) {
        super(message);
    }
}
