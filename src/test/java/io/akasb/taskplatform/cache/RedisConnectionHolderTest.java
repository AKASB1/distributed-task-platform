package io.akasb.taskplatform.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.akasb.taskplatform.support.Await;
import io.akasb.taskplatform.support.MutableClock;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/** Connection back-off without a Redis server (the connector always fails). Real connections: the Redis ITs. */
class RedisConnectionHolderTest {
    private final MutableClock clock = MutableClock.startingAt("2026-01-01T00:00:00Z");
    private final AtomicInteger attempts = new AtomicInteger();
    private final RedisConnectionHolder holder = new RedisConnectionHolder(() -> {
        attempts.incrementAndGet();
        throw new RedisConnectionException("connection refused");
    }, Duration.ofSeconds(1), Duration.ofSeconds(3), clock);

    @Test
    void creatingTheHolderDoesNotConnect() {
        assertThat(attempts).hasValue(0);
        assertThat(holder.isConnected()).isFalse();
    }

    @Test
    void failedConnectionIsRetriedAtMostOncePerInterval() {
        assertThatThrownBy(() -> holder.execute(c -> c.get("k")))
                .isInstanceOf(RedisUnavailableException.class)
                .hasCauseInstanceOf(RedisConnectionException.class);
        assertThat(attempts).hasValue(1);

        clock.advance(Duration.ofMillis(2999));
        assertThatThrownBy(() -> holder.execute(c -> c.get("k"))).isInstanceOf(RedisUnavailableException.class);
        assertThatThrownBy(() -> holder.execute(c -> c.get("k"))).isInstanceOf(RedisUnavailableException.class);
        assertThat(attempts).as("no attempt during the back-off").hasValue(1);

        clock.advance(Duration.ofMillis(1));
        assertThatThrownBy(() -> holder.execute(c -> c.get("k"))).isInstanceOf(RedisUnavailableException.class);
        assertThat(attempts).hasValue(2);
    }

    @Test
    void concurrentCallerWaitsForTheAttemptInProgressInsteadOfStartingAnother() throws Exception {
        CountDownLatch connecting = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        RedisConnectionHolder slow = new RedisConnectionHolder(blockingConnector(calls, connecting, release),
                Duration.ofSeconds(30), Duration.ofSeconds(3), clock);
        AtomicReference<Throwable> firstError = new AtomicReference<>();
        AtomicReference<Throwable> secondError = new AtomicReference<>();

        Thread first = caller(slow, firstError);
        first.start();
        assertThat(connecting.await(10, TimeUnit.SECONDS)).isTrue();
        Thread second = caller(slow, secondError);
        second.start();
        Await.until("second caller waits for the connection attempt", Duration.ofSeconds(10),
                () -> second.getState() == Thread.State.TIMED_WAITING);

        release.countDown();
        first.join(10_000);
        second.join(10_000);

        assertThat(firstError.get()).isInstanceOf(RedisUnavailableException.class);
        assertThat(secondError.get()).as("sees the failed attempt and its back-off")
                .isInstanceOf(RedisUnavailableException.class).hasMessageContaining("next connection attempt");
        assertThat(calls).as("one connection attempt only").hasValue(1);
    }

    @Test
    void callerGivesUpWaitingAfterConnectWait() throws Exception {
        CountDownLatch connecting = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        RedisConnectionHolder slow = new RedisConnectionHolder(blockingConnector(calls, connecting, release),
                Duration.ofMillis(50), Duration.ofSeconds(3), clock);
        AtomicReference<Throwable> firstError = new AtomicReference<>();
        AtomicReference<Throwable> secondError = new AtomicReference<>();
        Thread first = caller(slow, firstError);
        first.start();
        try {
            assertThat(connecting.await(10, TimeUnit.SECONDS)).isTrue();
            Thread second = caller(slow, secondError);
            second.start();
            second.join(10_000);

            assertThat(second.isAlive()).isFalse();
            assertThat(secondError.get()).isInstanceOf(RedisUnavailableException.class)
                    .hasMessageContaining("in progress");
        } finally {
            release.countDown();
            first.join(10_000);
        }
        assertThat(calls).hasValue(1);
    }

    private static Supplier<StatefulRedisConnection<String, String>> blockingConnector(
            AtomicInteger calls, CountDownLatch connecting, CountDownLatch release) {
        return () -> {
            calls.incrementAndGet();
            connecting.countDown();
            try {
                release.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new RedisConnectionException("connection refused");
        };
    }

    private static Thread caller(RedisConnectionHolder holder, AtomicReference<Throwable> error) {
        return new Thread(() -> {
            try {
                holder.execute(c -> c.get("k"));
            } catch (Throwable t) {
                error.set(t);
            }
        });
    }

    @Test
    void closedHolderNeverConnects() {
        holder.close();
        assertThatThrownBy(() -> holder.execute(c -> c.get("k"))).isInstanceOf(RedisUnavailableException.class);
        assertThat(attempts).hasValue(0);
    }

    @Test
    void clientsRejectInvalidSettings() {
        assertThatThrownBy(() -> RedisClients.create("redis://127.0.0.1:6379", Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RedisConnectionHolder(() -> null, Duration.ofSeconds(1),
                Duration.ofSeconds(-1), clock))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
