package io.akasb.taskplatform.worker;

import static org.assertj.core.api.Assertions.assertThat;

import io.akasb.taskplatform.domain.FailureKind;
import io.akasb.taskplatform.domain.TaskFailure;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

/**
 * {@code WorkerPool.classify}: only {@link NonRetryableTaskException} (and subclasses) is permanent; every other
 * throwable a handler raises is retryable and described with its simple class name.
 */
class WorkerPoolClassifyTest {

    /** A handler-specific permanent failure. */
    private static final class BadInputException extends NonRetryableTaskException {
        BadInputException(String message) {
            super(message);
        }
    }

    @Test
    void nonRetryableExceptionIsPermanentAndKeepsItsMessageVerbatim() {
        TaskFailure failure = WorkerPool.classify(new NonRetryableTaskException("bad input"));
        assertThat(failure.kind()).isEqualTo(FailureKind.NON_RETRYABLE);
        assertThat(failure.message()).isEqualTo("bad input");
        assertThat(failure.describe()).isEqualTo("NON_RETRYABLE: bad input");
    }

    @Test
    void nonRetryableExceptionWithCauseUsesItsOwnMessage() {
        TaskFailure failure = WorkerPool.classify(
                new NonRetryableTaskException("schema mismatch", new IllegalStateException("inner")));
        assertThat(failure.kind()).isEqualTo(FailureKind.NON_RETRYABLE);
        assertThat(failure.message()).isEqualTo("schema mismatch");
    }

    @Test
    void subclassesOfNonRetryableExceptionArePermanentToo() {
        TaskFailure failure = WorkerPool.classify(new BadInputException("field x missing"));
        assertThat(failure.kind()).isEqualTo(FailureKind.NON_RETRYABLE);
        assertThat(failure.message()).isEqualTo("field x missing");
    }

    @Test
    void nonRetryableExceptionWithoutMessageDescribesAsTheKind() {
        TaskFailure failure = WorkerPool.classify(new NonRetryableTaskException(null));
        assertThat(failure.kind()).isEqualTo(FailureKind.NON_RETRYABLE);
        assertThat(failure.message()).isEmpty();
        assertThat(failure.describe()).isEqualTo("NON_RETRYABLE");
    }

    @Test
    void retryableExceptionIsRetryableWithClassNamePrefix() {
        TaskFailure failure = WorkerPool.classify(new RetryableTaskException("try later"));
        assertThat(failure.kind()).isEqualTo(FailureKind.RETRYABLE);
        assertThat(failure.message()).isEqualTo("RetryableTaskException: try later");
        assertThat(failure.describe()).isEqualTo("RETRYABLE: RetryableTaskException: try later");
    }

    @Test
    void anyOtherRuntimeExceptionIsRetryable() {
        TaskFailure failure = WorkerPool.classify(new IllegalStateException("boom"));
        assertThat(failure.kind()).isEqualTo(FailureKind.RETRYABLE);
        assertThat(failure.message()).isEqualTo("IllegalStateException: boom");
    }

    @Test
    void checkedExceptionsAndErrorsAreRetryable() {
        assertThat(WorkerPool.classify(new IOException("disk"))).isEqualTo(
                new TaskFailure(FailureKind.RETRYABLE, "IOException: disk"));
        assertThat(WorkerPool.classify(new TimeoutException("slow"))).isEqualTo(
                new TaskFailure(FailureKind.RETRYABLE, "TimeoutException: slow"));
        assertThat(WorkerPool.classify(new InterruptedException("attempt stopped"))).isEqualTo(
                new TaskFailure(FailureKind.RETRYABLE, "InterruptedException: attempt stopped"));
        assertThat(WorkerPool.classify(new StackOverflowError("deep"))).isEqualTo(
                new TaskFailure(FailureKind.RETRYABLE, "StackOverflowError: deep"));
    }

    @Test
    void messagelessExceptionIsDescribedByItsSimpleClassName() {
        TaskFailure failure = WorkerPool.classify(new IllegalStateException());
        assertThat(failure.kind()).isEqualTo(FailureKind.RETRYABLE);
        assertThat(failure.message()).isEqualTo("IllegalStateException");
        assertThat(failure.describe()).isEqualTo("RETRYABLE: IllegalStateException");
    }

    @Test
    void aWrappedNonRetryableCauseIsNotUnwrapped() {
        TaskFailure failure = WorkerPool.classify(
                new RuntimeException("wrapper", new NonRetryableTaskException("inner")));
        assertThat(failure.kind()).as("only the thrown type counts").isEqualTo(FailureKind.RETRYABLE);
        assertThat(failure.message()).isEqualTo("RuntimeException: wrapper");
    }

    @Test
    void longMessagesAreTruncatedTo2000Characters() {
        TaskFailure failure = WorkerPool.classify(new IllegalArgumentException("m".repeat(5_000)));
        assertThat(failure.message()).hasSize(2000).startsWith("IllegalArgumentException: mmm");

        TaskFailure permanent = WorkerPool.classify(new NonRetryableTaskException("p".repeat(5_000)));
        assertThat(permanent.message()).hasSize(2000).isEqualTo("p".repeat(2000));
    }
}
