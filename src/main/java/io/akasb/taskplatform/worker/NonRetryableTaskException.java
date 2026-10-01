package io.akasb.taskplatform.worker;

/** Thrown by a {@link TaskHandler} for a permanent failure: the job goes to FAILED without retries. */
public class NonRetryableTaskException extends RuntimeException {
    public NonRetryableTaskException(String message) {
        super(message);
    }

    public NonRetryableTaskException(String message, Throwable cause) {
        super(message, cause);
    }
}
