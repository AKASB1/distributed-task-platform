package io.akasb.taskplatform.support;

import java.time.Duration;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/** Polls a condition until it holds or a timeout expires (no fixed sleeps in tests). */
public final class Await {
    private static final long POLL_NANOS = 2_000_000; // 2 ms

    private Await() {}

    public static void until(String description, Duration timeout, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - deadline > 0) {
                throw new AssertionError("timed out after " + timeout + " waiting for: " + description);
            }
            LockSupport.parkNanos(POLL_NANOS);
        }
    }

    /** Polls {@code supplier} until {@code accept} holds for its value and returns that value. */
    public static <T> T value(String description, Duration timeout, Supplier<T> supplier,
                              java.util.function.Predicate<T> accept) {
        long deadline = System.nanoTime() + timeout.toNanos();
        T last = supplier.get();
        while (!accept.test(last)) {
            if (System.nanoTime() - deadline > 0) {
                throw new AssertionError("timed out after " + timeout + " waiting for: " + description
                        + " (last value: " + last + ")");
            }
            LockSupport.parkNanos(POLL_NANOS);
            last = supplier.get();
        }
        return last;
    }
}
