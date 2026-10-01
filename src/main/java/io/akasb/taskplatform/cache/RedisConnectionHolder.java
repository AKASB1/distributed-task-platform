package io.akasb.taskplatform.cache;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One shared Redis connection, opened lazily and replaced after failures, so that Redis being down (at startup or
 * later) never stops the service and never costs more than one timeout per retry interval.
 *
 * <ul>
 *   <li>The first command connects. A failed connection attempt starts a back-off: until {@code retryInterval} has
 *       passed, commands fail at once with {@link RedisUnavailableException} instead of trying again.</li>
 *   <li>A command that fails for any reason other than an error reply from the server (timeout, closed connection)
 *       drops the connection and starts the same back-off; the next command after it reconnects.</li>
 *   <li>Only one thread connects at a time. The others wait for that attempt, at most {@code connectWait} (about
 *       the connect plus handshake timeout), and then use its result; if it takes longer they fail fast.</li>
 * </ul>
 * Callers treat every exception as "Redis unavailable" and fall back.
 */
public final class RedisConnectionHolder implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RedisConnectionHolder.class);

    private final Supplier<StatefulRedisConnection<String, String>> connector;
    private final Duration connectWait;
    private final Duration retryInterval;
    private final Clock clock;
    private final AtomicReference<StatefulRedisConnection<String, String>> connection = new AtomicReference<>();
    private final ReentrantLock connecting = new ReentrantLock();
    private volatile Instant retryNotBefore = Instant.MIN;
    private volatile boolean closed;

    /**
     * @param connector     opens a connection; must give up within about {@code connectWait}
     * @param connectWait   how long a thread waits for another thread's connection attempt
     * @param retryInterval minimum time between connection attempts after a failure
     */
    public RedisConnectionHolder(Supplier<StatefulRedisConnection<String, String>> connector, Duration connectWait,
                                 Duration retryInterval, Clock clock) {
        this.connector = Objects.requireNonNull(connector);
        this.connectWait = Objects.requireNonNull(connectWait);
        this.retryInterval = Objects.requireNonNull(retryInterval);
        this.clock = Objects.requireNonNull(clock);
        if (connectWait.isNegative()) throw new IllegalArgumentException("connectWait must not be negative");
        if (retryInterval.isNegative()) throw new IllegalArgumentException("retryInterval must not be negative");
    }

    /**
     * A holder whose connections come from {@code client} (the caller owns and shuts down the client), for a client
     * built by {@link RedisClients#create} with {@code timeout}.
     */
    public static RedisConnectionHolder of(RedisClient client, Duration timeout, Duration retryInterval, Clock clock) {
        Objects.requireNonNull(client);
        // connect timeout + handshake (HELLO) timeout
        return new RedisConnectionHolder(client::connect, timeout.multipliedBy(2), retryInterval, clock);
    }

    /**
     * Runs {@code command} with the synchronous command API of the shared connection.
     *
     * @throws RedisUnavailableException if there is no connection and none may be attempted now
     * @throws RuntimeException          whatever the command threw (Lettuce timeouts and connection errors)
     */
    public <T> T execute(Function<RedisCommands<String, String>, T> command) {
        StatefulRedisConnection<String, String> c = connection();
        try {
            return command.apply(c.sync());
        } catch (RedisCommandExecutionException e) {
            throw e; // the server answered with an error: the connection itself is fine
        } catch (RuntimeException e) {
            drop(c, e);
            throw e;
        }
    }

    /** Whether a connection is open right now (it may still fail on the next command). */
    public boolean isConnected() {
        StatefulRedisConnection<String, String> c = connection.get();
        return c != null && c.isOpen();
    }

    private StatefulRedisConnection<String, String> connection() {
        StatefulRedisConnection<String, String> c = connection.get();
        if (c != null && c.isOpen()) return c;
        checkMayConnect();
        lockForConnecting();
        try {
            c = connection.get();
            if (c != null && c.isOpen()) return c;
            if (c != null) {
                connection.compareAndSet(c, null);
                closeQuietly(c);
            }
            checkMayConnect();
            try {
                c = connector.get();
            } catch (RuntimeException e) {
                retryNotBefore = clock.instant().plus(retryInterval);
                throw new RedisUnavailableException("cannot connect to Redis: " + e.getMessage(), e);
            }
            if (closed) {
                closeQuietly(c);
                throw new RedisUnavailableException("Redis connection holder is closed");
            }
            connection.set(c);
            log.info("connected to Redis");
            return c;
        } finally {
            connecting.unlock();
        }
    }

    private void lockForConnecting() {
        try {
            if (connecting.tryLock(connectWait.toNanos(), TimeUnit.NANOSECONDS)) return;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        throw new RedisUnavailableException("Redis connection attempt in progress");
    }

    private void checkMayConnect() {
        if (closed) throw new RedisUnavailableException("Redis connection holder is closed");
        Instant notBefore = retryNotBefore;
        if (clock.instant().isBefore(notBefore)) {
            throw new RedisUnavailableException("Redis unavailable; next connection attempt after " + notBefore);
        }
    }

    private void drop(StatefulRedisConnection<String, String> c, RuntimeException cause) {
        retryNotBefore = clock.instant().plus(retryInterval);
        if (connection.compareAndSet(c, null)) {
            log.debug("dropping Redis connection after {}", cause.toString());
            closeQuietly(c);
        }
    }

    private static void closeQuietly(StatefulRedisConnection<String, String> c) {
        try {
            c.closeAsync();
        } catch (RuntimeException ignored) {
            // already broken
        }
    }

    @Override
    public void close() {
        closed = true;
        StatefulRedisConnection<String, String> c = connection.getAndSet(null);
        if (c != null) {
            try {
                c.close();
            } catch (RuntimeException e) {
                log.debug("closing Redis connection failed", e);
            }
        }
    }
}
