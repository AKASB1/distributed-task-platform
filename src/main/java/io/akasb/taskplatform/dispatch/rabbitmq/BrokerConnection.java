package io.akasb.taskplatform.dispatch.rabbitmq;

import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One AMQP connection, opened on first use. Once open, the client's automatic recovery keeps it alive across broker
 * restarts and network failures; until the first open succeeds, a failed attempt is retried at most once per
 * {@code retryInterval} and callers in between fail fast instead of each waiting for a connect timeout.
 *
 * <p>The connection also tracks {@code connection.blocked}: while the broker blocks publishers (memory or disk
 * alarm) {@link #blocked()} is true, so a publisher can refuse to write instead of hanging on the socket.
 */
final class BrokerConnection implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BrokerConnection.class);

    private final ConnectionFactory factory;
    private final String name;
    private final String target;
    private final long retryNanos;
    private final int closeTimeoutMillis;
    private final ReentrantLock lock = new ReentrantLock();
    private volatile Connection connection;
    private volatile boolean blocked;
    private volatile boolean closed;
    private long nextAttemptNanos; // guarded by lock

    BrokerConnection(ConnectionFactory factory, String name, String target, Duration retryInterval,
                     Duration closeTimeout) {
        this.factory = Objects.requireNonNull(factory);
        this.name = Objects.requireNonNull(name);
        this.target = Objects.requireNonNull(target);
        this.retryNanos = retryInterval.toNanos();
        this.closeTimeoutMillis = Math.toIntExact(closeTimeout.toMillis());
        this.nextAttemptNanos = System.nanoTime();
    }

    /**
     * The open (or recovering) connection. Opens it if this is the first use or the last attempt is old enough.
     *
     * @throws IllegalStateException if the broker cannot be reached or this connection was closed
     */
    Connection get() {
        Connection current = connection;
        if (current != null) return current;
        lock.lock();
        try {
            if (closed) throw new IllegalStateException("RabbitMQ connection " + name + " is closed");
            if (connection != null) return connection;
            long now = System.nanoTime();
            long wait = nextAttemptNanos - now;
            if (wait > 0) {
                throw new IllegalStateException("RabbitMQ at " + target + " is unreachable; next connection attempt in "
                        + Duration.ofNanos(wait).toMillis() + " ms");
            }
            try {
                Connection opened = factory.newConnection(name);
                watch(opened);
                connection = opened;
                log.info("connected to RabbitMQ at {} ({})", target, name);
                return opened;
            } catch (IOException | TimeoutException e) {
                nextAttemptNanos = now + retryNanos;
                log.warn("could not connect to RabbitMQ at {} ({}): {}", target, name, e.toString());
                throw new IllegalStateException("could not connect to RabbitMQ at " + target + ": " + e, e);
            }
        } finally {
            lock.unlock();
        }
    }

    /** True while the broker blocks this connection's publishes (resource alarm). */
    boolean blocked() {
        return blocked;
    }

    private void watch(Connection opened) {
        opened.addShutdownListener(cause -> {
            if (!cause.isInitiatedByApplication()) {
                log.warn("RabbitMQ connection {} lost ({}); automatic recovery will reconnect", name,
                        cause.getMessage());
            }
        });
        opened.addBlockedListener(new BlockedListener() {
            @Override
            public void handleBlocked(String reason) {
                blocked = true;
                log.warn("RabbitMQ blocked connection {}: {}", name, reason);
            }

            @Override
            public void handleUnblocked() {
                blocked = false;
                log.info("RabbitMQ unblocked connection {}", name);
            }
        });
        if (opened instanceof Recoverable recoverable) {
            recoverable.addRecoveryListener(new RecoveryListener() {
                @Override
                public void handleRecovery(Recoverable r) {
                    // A recovered connection starts unblocked; the broker sends connection.blocked again if needed.
                    blocked = false;
                    log.info("RabbitMQ connection {} recovered", name);
                }

                @Override
                public void handleRecoveryStarted(Recoverable r) {
                    log.info("RabbitMQ connection {} recovering", name);
                }
            });
        }
    }

    /** Closes the connection (and with it every channel). Idempotent; later {@link #get()} calls fail. */
    @Override
    public void close() {
        Connection current;
        lock.lock();
        try {
            closed = true;
            current = connection;
            connection = null;
        } finally {
            lock.unlock();
        }
        if (current == null) return;
        try {
            current.close(closeTimeoutMillis);
        } catch (IOException | RuntimeException e) {
            log.debug("closing RabbitMQ connection {} failed ({}); aborting it", name, e.toString());
            current.abort(closeTimeoutMillis);
        }
    }
}
