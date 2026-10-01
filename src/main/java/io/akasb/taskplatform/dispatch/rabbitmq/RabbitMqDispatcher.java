package io.akasb.taskplatform.dispatch.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.ShutdownSignalException;
import io.akasb.taskplatform.dispatch.JobDispatcher;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link JobDispatcher} on RabbitMQ (plain {@code amqp-client}). Each job queue {@code q} is backed by one durable,
 * classic RabbitMQ queue {@code queuePrefix + q}, declared on first use. A message is the job id as a UTF-8 UUID
 * string, published persistent on the default exchange. Unlike the in-memory queue, messages survive a restart of the
 * service, can be shared by several service instances, and are not de-duplicated.
 *
 * <p>Delivery semantics. A message is only a hint that a QUEUED job exists; PostgreSQL stays authoritative:
 * <ul>
 *   <li><b>Publish</b>: one publishing channel, guarded by a lock, without publisher confirms. If the publish fails
 *       (broker unreachable, channel closed, broker blocking publishers) {@link #dispatch} throws; the job stays QUEUED
 *       in the database and the reconciler dispatches it again after {@code redispatch-after}. A message the broker
 *       loses after accepting it is recovered the same way.</li>
 *   <li><b>Consume</b>: {@link #poll} starts one consumer per job queue on its own channel, with manual
 *       acknowledgements and {@code basicQos(prefetch)}, so at most {@code prefetch} unacknowledged deliveries wait in
 *       a local buffer. Poll takes the next buffered delivery and <i>acknowledges it before returning the id</i>
 *       (hand-off ack): from then on the job is the database's responsibility. A worker that dies after the hand-off
 *       but before claiming leaves a QUEUED job, which the reconciler re-dispatches; one that dies after claiming
 *       leaves a lease, which expires. Deliveries that were buffered but never handed off are unacknowledged, so the
 *       broker redelivers them when the consumer's channel or connection closes (shutdown, crash, network failure).</li>
 *   <li><b>Duplicates</b> are expected (redelivery after a connection loss, reconciler re-dispatch of a job still
 *       waiting in a long queue) and harmless: the worker's claim is a compare-and-set, so a stale id is dropped.</li>
 *   <li>A message whose body is not a UUID is acknowledged, dropped and logged.</li>
 * </ul>
 *
 * <p>{@link #durable()} is true, so the reconciler does not re-dispatch every QUEUED job at startup. Publishing and
 * consuming use separate connections (a broker resource alarm blocks publishing connections only, so consumers keep
 * acknowledging). Both connections open lazily and use automatic connection and topology recovery: consumers and
 * queue declarations are restored after a broker restart or network failure. While a connection is down, dispatch
 * and poll throw and {@link #depth} returns -1.
 */
public final class RabbitMqDispatcher implements JobDispatcher {
    private static final Logger log = LoggerFactory.getLogger(RabbitMqDispatcher.class);
    static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    static final Duration RECOVERY_INTERVAL = Duration.ofSeconds(2);
    static final Duration RPC_TIMEOUT = Duration.ofSeconds(10);
    static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(5);
    private static final AMQP.BasicProperties PERSISTENT = MessageProperties.PERSISTENT_TEXT_PLAIN;
    private static final int UUID_LENGTH = 36;

    /**
     * Connection and queue settings.
     *
     * @param queuePrefix prefix of the RabbitMQ queue that backs each job queue (may be empty)
     * @param prefetch    unacknowledged deliveries a consumer may buffer per job queue (1 to 65535)
     */
    public record Settings(String host, int port, String username, String password, String virtualHost,
                           String queuePrefix, int prefetch) {
        public Settings {
            if (host == null || host.isBlank()) throw new IllegalArgumentException("host is required");
            if (port < 1 || port > 65_535) throw new IllegalArgumentException("port must be between 1 and 65535");
            Objects.requireNonNull(username, "username");
            Objects.requireNonNull(password, "password");
            if (virtualHost == null || virtualHost.isEmpty()) {
                throw new IllegalArgumentException("virtualHost is required");
            }
            Objects.requireNonNull(queuePrefix, "queuePrefix");
            if (prefetch < 1 || prefetch > 65_535) {
                throw new IllegalArgumentException("prefetch must be between 1 and 65535");
            }
        }

        /** Never prints the password. */
        @Override
        public String toString() {
            return "Settings[host=" + host + ", port=" + port + ", username=" + username + ", password=****"
                    + ", virtualHost=" + virtualHost + ", queuePrefix=" + queuePrefix + ", prefetch=" + prefetch + "]";
        }

        String target() {
            return host + ":" + port + " vhost " + virtualHost;
        }
    }

    private record Pending(long deliveryTag, UUID jobId) { }

    private final Settings settings;
    private final BrokerConnection publisher;
    private final BrokerConnection consumer;
    private final ReentrantLock publishLock = new ReentrantLock();
    private Channel publishChannel; // guarded by publishLock
    /** RabbitMQ queues declared on the current publishing channel. */
    private final Set<String> declaredForPublish = ConcurrentHashMap.newKeySet();
    private final ReentrantLock consumerLock = new ReentrantLock();
    private final ConcurrentMap<String, QueueConsumer> consumers = new ConcurrentHashMap<>();
    private volatile boolean closed;

    /**
     * Creates the dispatcher and tries to connect at once, so a misconfiguration shows up in the startup log. A broker
     * that is not reachable yet is not an error: the connections open on first use.
     */
    public RabbitMqDispatcher(Settings settings) {
        this.settings = Objects.requireNonNull(settings, "settings");
        ConnectionFactory factory = connectionFactory(settings);
        this.publisher = new BrokerConnection(factory, "taskplatform-publisher", settings.target(), RECOVERY_INTERVAL,
                CLOSE_TIMEOUT);
        this.consumer = new BrokerConnection(factory, "taskplatform-consumer", settings.target(), RECOVERY_INTERVAL,
                CLOSE_TIMEOUT);
        log.info("RabbitMQ dispatcher: {}", settings);
        connectEagerly(publisher);
        connectEagerly(consumer);
    }

    private static ConnectionFactory connectionFactory(Settings s) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(s.host());
        factory.setPort(s.port());
        factory.setUsername(s.username());
        factory.setPassword(s.password());
        factory.setVirtualHost(s.virtualHost());
        factory.setAutomaticRecoveryEnabled(true);
        factory.setTopologyRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(RECOVERY_INTERVAL.toMillis());
        factory.setConnectionTimeout(Math.toIntExact(CONNECT_TIMEOUT.toMillis()));
        factory.setChannelRpcTimeout(Math.toIntExact(RPC_TIMEOUT.toMillis()));
        factory.setThreadFactory(Thread.ofPlatform().name("rabbitmq-", 0).daemon(true).factory());
        return factory;
    }

    private static void connectEagerly(BrokerConnection connection) {
        try {
            connection.get();
        } catch (RuntimeException e) {
            // BrokerConnection has logged the cause.
            log.info("RabbitMQ not reachable at startup; connecting on first use");
        }
    }

    /** The RabbitMQ queue that backs {@code queue}. */
    public String rabbitQueue(String queue) {
        return settings.queuePrefix() + Objects.requireNonNull(queue, "queue");
    }

    // ------------------------------------------------------------------------------------------- publish

    /**
     * Publishes {@code jobId} as a persistent message to the queue's RabbitMQ queue.
     *
     * @throws IllegalStateException if the broker is unreachable, blocks publishers, or the publish fails
     */
    @Override
    public void dispatch(String queue, UUID jobId) {
        Objects.requireNonNull(jobId, "jobId");
        String name = rabbitQueue(queue);
        if (closed) throw new IllegalStateException("RabbitMQ dispatcher is closed");
        if (publisher.blocked()) {
            throw new IllegalStateException("RabbitMQ is blocking publishers (resource alarm); job " + jobId
                    + " not published to " + name);
        }
        byte[] body = jobId.toString().getBytes(StandardCharsets.UTF_8);
        publishLock.lock();
        try {
            Channel channel = publishChannel();
            if (!declaredForPublish.contains(name)) {
                declare(channel, name);
                declaredForPublish.add(name);
            }
            channel.basicPublish("", name, PERSISTENT, body);
        } catch (IOException | ShutdownSignalException e) {
            throw new IllegalStateException("could not publish job " + jobId + " to RabbitMQ queue " + name + ": "
                    + e.getMessage(), e);
        } finally {
            publishLock.unlock();
        }
    }

    private Channel publishChannel() throws IOException {
        Channel channel = publishChannel;
        if (channel != null && !closedByChannelError(channel)) return channel;
        if (channel != null) {
            log.warn("RabbitMQ publishing channel was closed ({}); opening a new one", channel.getCloseReason());
            abortQuietly(channel);
        }
        publishChannel = null;
        declaredForPublish.clear();
        Channel created = publisher.get().createChannel();
        if (created == null) throw new IllegalStateException("no free RabbitMQ channel");
        publishChannel = created;
        return created;
    }

    /**
     * A channel closed by a channel-level error (for example a failed declare) is never recovered by the client and
     * must be replaced; one closed together with its connection is restored by automatic recovery.
     */
    private static boolean closedByChannelError(Channel channel) {
        if (channel.isOpen()) return false;
        ShutdownSignalException reason = channel.getCloseReason();
        return reason == null || !reason.isHardError();
    }

    private static void declare(Channel channel, String name) throws IOException {
        channel.queueDeclare(name, true, false, false, null);
    }

    // ------------------------------------------------------------------------------------------- consume

    /**
     * Hands out the next buffered job id of {@code queue}, acknowledging it first; empty after {@code timeout}. The
     * first call for a queue starts its consumer.
     *
     * @throws IllegalStateException if the consumer cannot be started (broker unreachable, dispatcher closed)
     */
    @Override
    public Optional<UUID> poll(String queue, Duration timeout) throws InterruptedException {
        if (Thread.interrupted()) throw new InterruptedException();
        QueueConsumer source = consumerFor(queue);
        Pending next = source.buffer.poll(Math.max(0, timeout.toNanos()), TimeUnit.NANOSECONDS);
        if (next == null) return Optional.empty();
        source.ack(next.deliveryTag());
        return Optional.of(next.jobId());
    }

    private QueueConsumer consumerFor(String queue) {
        QueueConsumer existing = consumers.get(queue);
        if (existing != null && !existing.retired) return existing;
        String name = rabbitQueue(queue);
        consumerLock.lock();
        try {
            if (closed) throw new IllegalStateException("RabbitMQ dispatcher is closed");
            existing = consumers.get(queue);
            if (existing != null) {
                if (!existing.retired) return existing;
                consumers.remove(queue, existing);
            }
            QueueConsumer started = startConsumer(queue, name);
            consumers.put(queue, started);
            return started;
        } finally {
            consumerLock.unlock();
        }
    }

    private QueueConsumer startConsumer(String queue, String name) {
        Channel channel = null;
        try {
            channel = consumer.get().createChannel();
            if (channel == null) throw new IllegalStateException("no free RabbitMQ channel");
            channel.basicQos(settings.prefetch());
            declare(channel, name);
            QueueConsumer started = new QueueConsumer(queue, name, channel);
            channel.basicConsume(name, false, started);
            log.info("consuming RabbitMQ queue {} for job queue {} (prefetch {})", name, queue, settings.prefetch());
            return started;
        } catch (IOException | ShutdownSignalException e) {
            if (channel != null) abortQuietly(channel);
            throw new IllegalStateException("could not start consuming RabbitMQ queue " + name + ": " + e.getMessage(),
                    e);
        }
    }

    /** One consumer per job queue: buffers deliveries until {@link #poll} hands them out. */
    private final class QueueConsumer extends DefaultConsumer {
        final String queue;
        final String rabbitQueue;
        final LinkedBlockingQueue<Pending> buffer = new LinkedBlockingQueue<>();
        private final ReentrantLock ackLock = new ReentrantLock();
        volatile boolean retired;

        QueueConsumer(String queue, String rabbitQueue, Channel channel) {
            super(channel);
            this.queue = queue;
            this.rabbitQueue = rabbitQueue;
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                   byte[] body) {
            UUID id = parseJobId(body);
            if (id == null) {
                log.warn("dropping malformed message on RabbitMQ queue {} ({} bytes, not a job id)", rabbitQueue,
                        body == null ? 0 : body.length);
                ack(envelope.getDeliveryTag());
                return;
            }
            buffer.add(new Pending(envelope.getDeliveryTag(), id));
        }

        /** The broker cancelled the consumer (for example, the queue was deleted). The next poll starts a new one. */
        @Override
        public void handleCancel(String consumerTag) {
            log.warn("RabbitMQ cancelled the consumer of queue {}; it is restarted on the next poll", rabbitQueue);
            retire();
        }

        @Override
        public void handleShutdownSignal(String consumerTag, ShutdownSignalException signal) {
            // Unacknowledged deliveries return to the queue when the channel closes: drop the local copies.
            buffer.clear();
            if (!signal.isHardError() && !signal.isInitiatedByApplication()) {
                // A channel-level error is not recovered automatically; a connection failure is.
                log.warn("RabbitMQ consumer channel of queue {} closed ({}); it is restarted on the next poll",
                        rabbitQueue, signal.getMessage());
                retire();
            }
        }

        /** Acknowledges one delivery. A failure is harmless: the channel is gone and the broker redelivers it. */
        void ack(long deliveryTag) {
            ackLock.lock();
            try {
                getChannel().basicAck(deliveryTag, false);
            } catch (IOException | ShutdownSignalException e) {
                log.debug("ack of delivery {} on RabbitMQ queue {} failed ({}); the broker will redeliver it",
                        deliveryTag, rabbitQueue, e.toString());
            } finally {
                ackLock.unlock();
            }
        }

        private void retire() {
            retired = true;
            consumers.remove(queue, this);
            declaredForPublish.remove(rabbitQueue);
            buffer.clear();
            abortQuietly(getChannel());
        }

        /** Cancels the consumer and closes its channel; buffered deliveries go back to the queue. */
        void stop() {
            retired = true;
            Channel channel = getChannel();
            String tag = getConsumerTag();
            try {
                if (tag != null && channel.isOpen()) channel.basicCancel(tag);
            } catch (IOException | ShutdownSignalException e) {
                log.debug("cancelling consumer of RabbitMQ queue {} failed: {}", rabbitQueue, e.toString());
            }
            closeQuietly(channel);
            buffer.clear();
        }
    }

    /** The job id in {@code body}, or null if it is not a canonical 36-character UUID string. */
    static UUID parseJobId(byte[] body) {
        if (body == null || body.length != UUID_LENGTH) return null;
        try {
            return UUID.fromString(new String(body, StandardCharsets.UTF_8));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    // ------------------------------------------------------------------------------------------- other

    /** Messages are persistent in a durable queue: they survive a restart of this process (and of the broker). */
    @Override
    public boolean durable() {
        return true;
    }

    /**
     * Ready messages in the RabbitMQ queue (passive declare) plus deliveries buffered locally and not yet handed out;
     * 0 if the queue does not exist yet, -1 if the broker cannot be asked. Messages buffered by other service
     * instances are not counted.
     */
    @Override
    public long depth(String queue) {
        String name = rabbitQueue(queue);
        QueueConsumer local = consumers.get(queue);
        long buffered = local == null ? 0 : local.buffer.size();
        Channel channel = null;
        try {
            if (closed) return -1;
            channel = consumer.get().createChannel();
            if (channel == null) return -1;
            return channel.queueDeclarePassive(name).getMessageCount() + buffered;
        } catch (IOException e) {
            if (isNotFound(e)) return buffered;
            log.debug("depth of RabbitMQ queue {} unavailable: {}", name, e.toString());
            return -1;
        } catch (RuntimeException e) {
            log.debug("depth of RabbitMQ queue {} unavailable: {}", name, e.toString());
            return -1;
        } finally {
            if (channel != null) closeQuietly(channel);
        }
    }

    private static boolean isNotFound(IOException e) {
        return e.getCause() instanceof ShutdownSignalException signal && !signal.isHardError()
                && signal.getReason() instanceof AMQP.Channel.Close close && close.getReplyCode() == AMQP.NOT_FOUND;
    }

    /**
     * Cancels the consumers, closes the channels and both connections. Deliveries buffered but not handed out are
     * returned to their queues by the broker. Idempotent.
     */
    @Override
    public void close() {
        consumerLock.lock();
        try {
            if (closed) return;
            closed = true;
            consumers.values().forEach(QueueConsumer::stop);
            consumers.clear();
        } finally {
            consumerLock.unlock();
        }
        publishLock.lock();
        try {
            if (publishChannel != null) closeQuietly(publishChannel);
            publishChannel = null;
        } finally {
            publishLock.unlock();
        }
        consumer.close();
        publisher.close();
        log.info("RabbitMQ dispatcher closed");
    }

    private static void closeQuietly(Channel channel) {
        try {
            if (channel.isOpen()) channel.close();
            else channel.abort();
        } catch (IOException | TimeoutException | RuntimeException e) {
            abortQuietly(channel);
        }
    }

    private static void abortQuietly(Channel channel) {
        try {
            channel.abort();
        } catch (IOException | RuntimeException e) {
            log.debug("aborting RabbitMQ channel failed: {}", e.toString());
        }
    }
}
