package io.akasb.taskplatform.api;

import java.time.Duration;
import java.util.Objects;

/**
 * Admission control for job submission, per client. Implementations backed by an external store fail open: when the
 * store is unavailable the request is allowed.
 */
public interface RateLimiter {

    /** Counts one request of {@code clientKey} and says whether it may proceed. */
    Decision tryAcquire(String clientKey);

    /**
     * @param allowed    whether the request may proceed
     * @param retryAfter for a rejected request, how long until the client may try again; zero when allowed
     */
    record Decision(boolean allowed, Duration retryAfter) {
        private static final Decision ALLOWED = new Decision(true, Duration.ZERO);

        public Decision {
            Objects.requireNonNull(retryAfter, "retryAfter");
            if (retryAfter.isNegative()) throw new IllegalArgumentException("retryAfter must not be negative");
            if (allowed && !retryAfter.isZero()) throw new IllegalArgumentException("an allowed request has no retryAfter");
        }

        public static Decision allow() {
            return ALLOWED;
        }

        public static Decision reject(Duration retryAfter) {
            return new Decision(false, retryAfter);
        }
    }

    /** Never limits. */
    static RateLimiter unlimited() {
        return clientKey -> Decision.allow();
    }
}
