package io.akasb.taskplatform.worker;

/**
 * Pluggable task implementation, selected by the job's {@code type}.
 *
 * <p>Contract: return a result (serialised to JSON, may be null) on success; throw
 * {@link NonRetryableTaskException} for permanent failures and anything else for retryable ones. Long handlers
 * should call {@link TaskContext#reportProgress} and must stop promptly when interrupted (timeout, cancellation,
 * lease loss, shutdown).
 */
public interface TaskHandler {

    /** The job type this handler serves, for example {@code "sleep"}. */
    String type();

    Object handle(TaskContext context) throws Exception;
}
