package io.akasb.taskplatform.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.akasb.taskplatform.api.RateLimiter.Decision;
import io.akasb.taskplatform.support.MutableClock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Fixed windows aligned to the epoch, driven by a {@link MutableClock}. */
class InMemoryRateLimiterTest {
    /** 2026-01-01T00:00:00Z is a multiple of the 2 s window, so the clock starts exactly at a window boundary. */
    private static final Instant WINDOW_START = Instant.parse("2026-01-01T00:00:00Z");
    private MutableClock clock;
    private InMemoryRateLimiter limiter;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(WINDOW_START);
        limiter = new InMemoryRateLimiter(3, Duration.ofSeconds(2), clock);
    }

    private void at(long millisIntoFirstWindow) {
        clock.set(WINDOW_START.plusMillis(millisIntoFirstWindow));
    }

    @Test
    void allowsUpToTheLimitThenRejectsUntilTheNextWindow() {
        assertThat(limiter.tryAcquire("a")).isEqualTo(Decision.allow());
        at(500);
        assertThat(limiter.tryAcquire("a").allowed()).isTrue();
        assertThat(limiter.tryAcquire("a").allowed()).isTrue();

        Decision fourth = limiter.tryAcquire("a");
        assertThat(fourth.allowed()).isFalse();
        assertThat(fourth.retryAfter()).isEqualTo(Duration.ofMillis(1500));
    }

    @Test
    void retryAfterIsTheTimeToTheNextWindowBoundary() {
        for (int i = 0; i < 3; i++) limiter.tryAcquire("a");
        at(250);
        assertThat(limiter.tryAcquire("a").retryAfter()).isEqualTo(Duration.ofMillis(1750));
        at(1999);
        assertThat(limiter.tryAcquire("a").retryAfter()).isEqualTo(Duration.ofMillis(1));
    }

    @Test
    void lastMillisecondBelongsToTheOldWindowAndTheBoundaryStartsANewOne() {
        at(1999);
        for (int i = 0; i < 3; i++) assertThat(limiter.tryAcquire("a").allowed()).isTrue();
        assertThat(limiter.tryAcquire("a").allowed()).isFalse();

        at(2000);
        for (int i = 0; i < 3; i++) assertThat(limiter.tryAcquire("a").allowed()).as("request %d", i).isTrue();
        Decision rejected = limiter.tryAcquire("a");
        assertThat(rejected.allowed()).isFalse();
        assertThat(rejected.retryAfter()).isEqualTo(Duration.ofSeconds(2));
    }

    @Test
    void rejectedRequestsStillCountWithinTheWindow() {
        for (int i = 0; i < 10; i++) limiter.tryAcquire("a");
        clock.advance(Duration.ofSeconds(2));
        assertThat(limiter.tryAcquire("a").allowed()).as("a new window starts from zero").isTrue();
    }

    @Test
    void clientsAreCountedSeparately() {
        for (int i = 0; i < 3; i++) limiter.tryAcquire("a");
        assertThat(limiter.tryAcquire("a").allowed()).isFalse();

        assertThat(limiter.tryAcquire("b").allowed()).isTrue();
        assertThat(limiter.tryAcquire("b").allowed()).isTrue();
        assertThat(limiter.tryAcquire("b").allowed()).isTrue();
        assertThat(limiter.tryAcquire("b").allowed()).isFalse();
        assertThat(limiter.tryAcquire("c").allowed()).isTrue();
    }

    @Test
    void aClockSteppingBackKeepsCountingInTheNewerWindow() {
        at(2100);
        for (int i = 0; i < 3; i++) limiter.tryAcquire("a");
        at(1900);
        assertThat(limiter.tryAcquire("a").allowed()).isFalse();
    }

    @Test
    void exactlyTheLimitIsAllowedUnderConcurrency() throws Exception {
        int threads = 8;
        int perThread = 250;
        int limit = 500;
        InMemoryRateLimiter shared = new InMemoryRateLimiter(limit, Duration.ofSeconds(2), clock);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Integer>> results = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                results.add(pool.submit(() -> {
                    start.await();
                    int allowed = 0;
                    for (int i = 0; i < perThread; i++) {
                        if (shared.tryAcquire("hot").allowed()) allowed++;
                    }
                    return allowed;
                }));
            }
            start.countDown();
            int total = 0;
            for (Future<Integer> f : results) total += f.get(30, TimeUnit.SECONDS);
            assertThat(total).isEqualTo(limit);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void rejectsInvalidSettings() {
        assertThatThrownBy(() -> new InMemoryRateLimiter(0, Duration.ofSeconds(1), clock))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new InMemoryRateLimiter(1, Duration.ZERO, clock))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void decisionsAreConsistent() {
        assertThat(Decision.allow().retryAfter()).isZero();
        assertThat(RateLimiter.unlimited().tryAcquire("x")).isEqualTo(Decision.allow());
        assertThatThrownBy(() -> new Decision(true, Duration.ofSeconds(1))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Decision.reject(Duration.ofMillis(-1))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void windowsAreAlignedToTheEpoch() {
        FixedWindow window = new FixedWindow(Duration.ofSeconds(2));
        long start = WINDOW_START.toEpochMilli();
        assertThat(window.index(start)).isEqualTo(window.index(start + 1999)).isEqualTo(start / 2000);
        assertThat(window.index(start + 2000)).isEqualTo(start / 2000 + 1);
        assertThat(window.untilNext(start)).isEqualTo(Duration.ofSeconds(2));
        assertThat(window.index(-1)).isEqualTo(-1);
    }
}
