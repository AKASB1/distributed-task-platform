package io.akasb.taskplatform.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Failure classification value: message normalisation, truncation, and the stored description. */
class TaskFailureTest {

    @Test
    void requiresAKind() {
        assertThatThrownBy(() -> new TaskFailure(null, "x"))
                .isInstanceOf(NullPointerException.class).hasMessage("kind");
    }

    @ParameterizedTest
    @EnumSource(FailureKind.class)
    void describeIsKindColonMessage(FailureKind kind) {
        assertThat(new TaskFailure(kind, "disk full").describe()).isEqualTo(kind.name() + ": disk full");
    }

    @ParameterizedTest
    @EnumSource(FailureKind.class)
    void nullOrEmptyMessageDescribesAsTheKindAlone(FailureKind kind) {
        TaskFailure fromNull = new TaskFailure(kind, null);
        assertThat(fromNull.message()).isEmpty();
        assertThat(fromNull.describe()).isEqualTo(kind.name());

        assertThat(new TaskFailure(kind, "").describe()).isEqualTo(kind.name());
    }

    @Test
    void messageIsTruncatedTo2000Characters() {
        String exact = "x".repeat(2000);
        assertThat(new TaskFailure(FailureKind.RETRYABLE, exact).message()).isEqualTo(exact);

        String longer = "y".repeat(1999) + "ZZ";
        TaskFailure truncated = new TaskFailure(FailureKind.RETRYABLE, longer);
        assertThat(truncated.message()).hasSize(2000).endsWith("yZ");
        assertThat(truncated.describe()).isEqualTo("RETRYABLE: " + truncated.message());

        assertThat(new TaskFailure(FailureKind.TIMEOUT, "z".repeat(100_000)).message()).hasSize(2000);
    }

    @Test
    void blankMessageIsKeptVerbatim() {
        assertThat(new TaskFailure(FailureKind.RETRYABLE, " ").describe()).isEqualTo("RETRYABLE:  ");
    }

    @Test
    void equalityIsByValueAfterNormalisation() {
        assertThat(new TaskFailure(FailureKind.TIMEOUT, null)).isEqualTo(new TaskFailure(FailureKind.TIMEOUT, ""));
        assertThat(new TaskFailure(FailureKind.TIMEOUT, "a")).isNotEqualTo(new TaskFailure(FailureKind.RETRYABLE, "a"));
    }

    @Test
    void onlyNonRetryableIsNotRetryable() {
        for (FailureKind kind : FailureKind.values()) {
            assertThat(kind.retryable()).as(kind.name()).isEqualTo(kind != FailureKind.NON_RETRYABLE);
        }
    }
}
