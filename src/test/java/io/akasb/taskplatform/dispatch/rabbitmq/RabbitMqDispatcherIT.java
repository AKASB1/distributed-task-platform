package io.akasb.taskplatform.dispatch.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.akasb.taskplatform.support.Await;
import io.akasb.taskplatform.support.RabbitMqTestContainer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * {@link RabbitMqDispatcher} against a real broker (Testcontainers, skipped without Docker): round trip, FIFO and
 * isolation per queue, timeouts, duplicates, depth, malformed messages, redelivery of prefetched messages, automatic
 * recovery, a deleted queue, a blocked broker, and a multi-producer/multi-consumer run across two instances.
 *
 * <p>All tests share one broker; each uses its own queue prefix, so they cannot see each other's messages.
 */
@Testcontainers(disabledWithoutDocker = true)
@Timeout(120)
class RabbitMqDispatcherIT {
    @org.testcontainers.junit.jupiter.Container
    static final RabbitMQContainer BROKER = RabbitMqTestContainer.create();

    private static final Duration WAIT = Duration.ofSeconds(20);
    private static final Duration SHORT = Duration.ofMillis(200);
    private static final AtomicInteger PREFIXES = new AtomicInteger();

    private final List<RabbitMqDispatcher> dispatchers = new ArrayList<>();
    private String prefix;
    private Connection admin;
    private ExecutorService pool;

    @BeforeEach
    void setUp(TestInfo info) {
        prefix = "it" + PREFIXES.incrementAndGet() + "-" + info.getTestMethod().orElseThrow().getName() + ".";
    }

    @AfterEach
    void tearDown() throws Exception {
        if (pool != null) {
            pool.shutdownNow();
            assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
        dispatchers.forEach(RabbitMqDispatcher::close);
        if (admin != null && admin.isOpen()) admin.close();
    }

    private RabbitMqDispatcher dispatcher(int prefetch) {
        RabbitMqDispatcher d = new RabbitMqDispatcher(RabbitMqTestContainer.settings(BROKER, prefix, prefetch));
        dispatchers.add(d);
        return d;
    }

    private RabbitMqDispatcher dispatcher() {
        return dispatcher(16);
    }

    private static UUID id(long n) {
        return new UUID(0x7AB817C0L, n);
    }

    // ------------------------------------------------------------------------------------------- basics

    @Test
    void roundTrip() throws Exception {
        RabbitMqDispatcher d = dispatcher();
        d.dispatch("q", id(1));

        assertThat(d.poll("q", WAIT)).contains(id(1));
        assertThat(d.poll("q", SHORT)).isEmpty();

        d.close();
        assertThat(ready("q")).as("acknowledged on hand-off, so nothing returns to the queue on close").isZero();
    }

    @Test
    void deliversInFifoOrderPerQueueAndKeepsQueuesIsolated() throws Exception {
        RabbitMqDispatcher d = dispatcher();
        for (int i = 0; i < 50; i++) {
            d.dispatch("a", id(i));
            d.dispatch("b", id(1_000 + i));
        }

        for (int i = 0; i < 50; i++) assertThat(d.poll("a", WAIT)).contains(id(i));
        assertThat(d.poll("a", SHORT)).isEmpty();
        for (int i = 0; i < 50; i++) assertThat(d.poll("b", WAIT)).contains(id(1_000 + i));
        assertThat(d.poll("b", SHORT)).isEmpty();
        assertThat(d.poll("c", SHORT)).isEmpty();
        assertThat(d.rabbitQueue("a")).isEqualTo(prefix + "a");
    }

    @Test
    void pollReturnsEmptyAfterTheTimeout() throws Exception {
        RabbitMqDispatcher d = dispatcher();
        assertThat(d.poll("empty", Duration.ofMillis(10))).isEmpty(); // starts the consumer

        Duration timeout = Duration.ofMillis(200);
        long start = System.nanoTime();
        Optional<UUID> got = d.poll("empty", timeout);
        long elapsed = System.nanoTime() - start;

        assertThat(got).isEmpty();
        assertThat(elapsed).as("waited at least the timeout").isGreaterThanOrEqualTo(timeout.toNanos());
        assertThat(elapsed).as("but not much longer").isLessThan(TimeUnit.SECONDS.toNanos(5));

        start = System.nanoTime();
        assertThat(d.poll("empty", Duration.ZERO)).isEmpty();
        assertThat(d.poll("empty", Duration.ofSeconds(-5))).isEmpty();
        assertThat(System.nanoTime() - start).as("zero and negative timeouts do not block")
                .isLessThan(TimeUnit.SECONDS.toNanos(2));
    }

    @Test
    void aDuplicateDispatchIsDeliveredTwice() throws Exception {
        // No de-duplication: the second copy is harmless because the worker's claim is a compare-and-set.
        RabbitMqDispatcher d = dispatcher();
        d.dispatch("q", id(7));
        d.dispatch("q", id(7));

        assertThat(d.poll("q", WAIT)).contains(id(7));
        assertThat(d.poll("q", WAIT)).contains(id(7));
        assertThat(d.poll("q", SHORT)).isEmpty();
    }

    @Test
    void isDurable() {
        assertThat(dispatcher().durable()).isTrue();
    }

    @Test
    void depthCountsReadyMessagesPlusLocallyBufferedDeliveries() throws Exception {
        RabbitMqDispatcher producer = dispatcher();
        RabbitMqDispatcher consumer = dispatcher();
        assertThat(producer.depth("q")).as("queue not declared yet").isZero();

        for (int i = 0; i < 3; i++) producer.dispatch("q", id(i));
        Await.value("three ready messages", WAIT, () -> producer.depth("q"), n -> n == 3);

        assertThat(consumer.poll("q", WAIT)).contains(id(0));
        // The consumer prefetched the other two: no longer ready in the broker, but waiting in its local buffer.
        Await.until("two deliveries buffered by the consumer", WAIT,
                () -> readyUnchecked("q") == 0 && consumer.depth("q") == 2);
        assertThat(producer.depth("q")).as("another instance sees only the broker").isZero();

        assertThat(consumer.poll("q", WAIT)).contains(id(1));
        assertThat(consumer.poll("q", WAIT)).contains(id(2));
        assertThat(consumer.depth("q")).isZero();
    }

    @Test
    void malformedMessagesAreAcknowledgedAndDropped() throws Exception {
        RabbitMqDispatcher d = dispatcher();
        try (Channel channel = admin().createChannel()) {
            channel.queueDeclare(prefix + "q", true, false, false, null);
            channel.basicPublish("", prefix + "q", null, "not-a-uuid".getBytes(StandardCharsets.UTF_8));
            channel.basicPublish("", prefix + "q", null, new byte[0]);
            channel.basicPublish("", prefix + "q", null,
                    "zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz".getBytes(StandardCharsets.UTF_8));
        }
        d.dispatch("q", id(1));

        assertThat(d.poll("q", WAIT)).as("the malformed messages are skipped").contains(id(1));
        assertThat(d.poll("q", SHORT)).isEmpty();

        d.close();
        assertThat(ready("q")).as("malformed messages were acknowledged, not returned to the queue").isZero();
    }

    @Test
    void afterCloseDispatchAndPollFailAndDepthIsUnknown() throws Exception {
        RabbitMqDispatcher d = dispatcher();
        d.dispatch("q", id(1));
        d.close();
        d.close(); // idempotent

        assertThatThrownBy(() -> d.dispatch("q", id(2))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> d.poll("q", SHORT)).isInstanceOf(IllegalStateException.class);
        assertThat(d.depth("q")).isEqualTo(-1);
        Await.until("the message published before close is kept by the broker", WAIT, () -> readyUnchecked("q") == 1);
    }

    // ------------------------------------------------------------------------------------------- redelivery

    @Test
    void deliveriesPrefetchedByAClosedDispatcherAreRedeliveredToANewOne() throws Exception {
        int prefetch = 4;
        RabbitMqDispatcher first = dispatcher(prefetch);
        for (int i = 0; i < 10; i++) first.dispatch("q", id(i));

        assertThat(first.poll("q", WAIT)).contains(id(0));
        // id 0 was handed off (acknowledged); the consumer now buffers `prefetch` unacknowledged deliveries.
        Await.until("prefetch buffer filled", WAIT, () -> readyUnchecked("q") == 10 - 1 - prefetch);
        assertThat(first.depth("q")).isEqualTo(9);

        first.close();
        Await.until("buffered deliveries returned to the queue", WAIT, () -> readyUnchecked("q") == 9);

        RabbitMqDispatcher second = dispatcher(prefetch);
        Set<UUID> received = new HashSet<>();
        for (int i = 0; i < 9; i++) received.add(second.poll("q", WAIT).orElseThrow());
        Set<UUID> expected = new HashSet<>();
        for (int i = 1; i < 10; i++) expected.add(id(i));
        assertThat(received).isEqualTo(expected);
        assertThat(second.poll("q", SHORT)).as("the handed-off id 0 is not redelivered").isEmpty();
    }

    @Test
    void recoversAfterTheBrokerClosesTheConnections() throws Exception {
        RabbitMqDispatcher d = dispatcher();
        d.dispatch("q", id(1));
        assertThat(d.poll("q", WAIT)).contains(id(1));

        exec("rabbitmqctl", "close_all_connections", "integration test");

        // Publishes are refused while the connection is down (a publish racing the close may be lost: the
        // reconciler covers that in the platform), then accepted again once automatic recovery has reconnected.
        AtomicLong probe = new AtomicLong(100);
        Await.until("dispatch refused while disconnected", WAIT, () -> !tryDispatch(d, "q", id(probe.incrementAndGet())));
        Await.until("dispatch accepted after recovery", WAIT, () -> tryDispatch(d, "q", id(2)));
        // The recovered consumer delivers it (probe ids from before the close may come first).
        Await.until("id 2 delivered by the recovered consumer", WAIT,
                () -> pollUnchecked(d, "q", Duration.ofMillis(50)).filter(id(2)::equals).isPresent());
    }

    @Test
    void aQueueDeletedOnTheBrokerIsDeclaredAgain() throws Exception {
        RabbitMqDispatcher d = dispatcher();
        d.dispatch("q", id(1));
        assertThat(d.poll("q", WAIT)).contains(id(1));

        try (Channel channel = admin().createChannel()) {
            channel.queueDelete(prefix + "q");
        }

        // The broker cancels the consumer; the next poll starts a new one and the queue is declared again.
        Await.until("queue usable again", WAIT, () -> {
            tryDispatch(d, "q", id(2));
            return pollUnchecked(d, "q", Duration.ofMillis(50)).isPresent();
        });
        assertThat(readyUnchecked("q")).as("queue exists again").isGreaterThanOrEqualTo(0);
    }

    @Test
    void dispatchFailsFastWhileTheBrokerBlocksPublishers() throws Exception {
        RabbitMqDispatcher d = dispatcher();
        d.dispatch("q", id(0));
        assertThat(d.poll("q", WAIT)).contains(id(0));

        List<UUID> accepted = new ArrayList<>();
        AtomicLong next = new AtomicLong(100);
        exec("rabbitmqctl", "set_vm_memory_high_watermark", "0"); // memory alarm: the broker blocks publishers
        try {
            // Until the client has been told (connection.blocked), publishes still go out; they are delivered later.
            Await.until("dispatch refused with a blocked broker", WAIT, () -> {
                UUID u = id(next.incrementAndGet());
                try {
                    d.dispatch("q", u);
                    accepted.add(u);
                    return false;
                } catch (IllegalStateException e) {
                    assertThat(e).hasMessageContaining("blocking publishers");
                    return true;
                }
            });
        } finally {
            exec("rabbitmqctl", "set_vm_memory_high_watermark", "0.6");
        }

        Await.until("dispatch accepted after the alarm cleared", WAIT, () -> tryDispatch(d, "q", id(1)));
        Set<UUID> expected = new HashSet<>(accepted);
        expected.add(id(1));
        Set<UUID> received = new HashSet<>();
        Await.until("every accepted id delivered", WAIT, () -> {
            pollUnchecked(d, "q", Duration.ofMillis(50)).ifPresent(received::add);
            return received.containsAll(expected);
        });
    }

    // ------------------------------------------------------------------------------------------- threads

    @Test
    void concurrentProducersAndConsumersOnTwoInstancesDeliverEveryIdAndLoseNone() throws Exception {
        int producers = 4;
        int perProducer = 250;
        int consumers = 4;
        int total = producers * perProducer;
        List<RabbitMqDispatcher> instances = List.of(dispatcher(8), dispatcher(8));
        instances.forEach(i -> pollUnchecked(i, "load", Duration.ZERO)); // declare the queue, start both consumers
        pool = Executors.newFixedThreadPool(producers + consumers);
        CyclicBarrier start = new CyclicBarrier(producers + consumers);
        Map<UUID, AtomicInteger> deliveries = new ConcurrentHashMap<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);

        List<Future<?>> producerFutures = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            final long producer = p;
            RabbitMqDispatcher via = instances.get(p % instances.size());
            producerFutures.add(pool.submit(() -> {
                start.await(10, TimeUnit.SECONDS);
                for (int i = 0; i < perProducer; i++) via.dispatch("load", new UUID(producer, i));
                return null;
            }));
        }
        List<Future<List<UUID>>> consumerFutures = new ArrayList<>();
        for (int c = 0; c < consumers; c++) {
            RabbitMqDispatcher from = instances.get(c % instances.size());
            consumerFutures.add(pool.submit(() -> {
                List<UUID> mine = new ArrayList<>();
                start.await(10, TimeUnit.SECONDS);
                while (deliveries.size() < total && System.nanoTime() < deadline) {
                    Optional<UUID> got = from.poll("load", Duration.ofMillis(20));
                    if (got.isEmpty()) continue;
                    mine.add(got.get());
                    deliveries.computeIfAbsent(got.get(), k -> new AtomicInteger()).incrementAndGet();
                }
                return mine;
            }));
        }

        for (Future<?> f : producerFutures) f.get(60, TimeUnit.SECONDS);
        List<List<UUID>> perConsumer = new ArrayList<>();
        for (Future<List<UUID>> f : consumerFutures) perConsumer.add(f.get(70, TimeUnit.SECONDS));

        Set<UUID> expected = new HashSet<>();
        for (long p = 0; p < producers; p++) {
            for (int i = 0; i < perProducer; i++) expected.add(new UUID(p, i));
        }
        assertThat(deliveries.keySet()).as("every id delivered at least once, none lost").isEqualTo(expected);
        // No channel closed during the run, so the broker had no reason to redeliver anything.
        assertThat(deliveries.values()).allSatisfy(n -> assertThat(n.get()).isEqualTo(1));
        for (int i = 0; i < instances.size(); i++) {
            int received = 0;
            for (int c = i; c < consumers; c += instances.size()) received += perConsumer.get(c).size();
            assertThat(received).as("instance %d received work", i).isPositive();
        }

        // Each consumer sees any one producer's ids in publication order.
        for (List<UUID> mine : perConsumer) {
            Map<Long, Long> lastSeen = new HashMap<>();
            for (UUID u : mine) {
                Long previous = lastSeen.put(u.getMostSignificantBits(), u.getLeastSignificantBits());
                if (previous != null) assertThat(u.getLeastSignificantBits()).isGreaterThan(previous);
            }
        }

        instances.forEach(RabbitMqDispatcher::close);
        assertThat(ready("load")).as("every delivery was acknowledged").isZero();
    }

    // ------------------------------------------------------------------------------------------- helpers

    /** Ready messages in the RabbitMQ queue behind job queue {@code queue}, asked over a separate connection. */
    private long ready(String queue) throws IOException, TimeoutException {
        try (Channel channel = admin().createChannel()) {
            return channel.queueDeclarePassive(prefix + queue).getMessageCount();
        }
    }

    private long readyUnchecked(String queue) {
        try {
            return ready(queue);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (TimeoutException e) {
            throw new IllegalStateException(e);
        }
    }

    private Connection admin() throws IOException, TimeoutException {
        if (admin == null || !admin.isOpen()) {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(BROKER.getHost());
            factory.setPort(BROKER.getAmqpPort());
            factory.setUsername(BROKER.getAdminUsername());
            factory.setPassword(BROKER.getAdminPassword());
            factory.setAutomaticRecoveryEnabled(false);
            admin = factory.newConnection("integration-test-admin");
        }
        return admin;
    }

    private static boolean tryDispatch(RabbitMqDispatcher d, String queue, UUID id) {
        try {
            d.dispatch(queue, id);
            return true;
        } catch (IllegalStateException e) {
            return false;
        }
    }

    private static Optional<UUID> pollUnchecked(RabbitMqDispatcher d, String queue, Duration timeout) {
        try {
            return d.poll(queue, timeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (IllegalStateException e) {
            return Optional.empty(); // consumer could not start (broker unavailable): try again
        }
    }

    private static void exec(String... command) throws IOException, InterruptedException {
        Container.ExecResult result = BROKER.execInContainer(command);
        assertThat(result.getExitCode()).as(String.join(" ", command) + ": " + result.getStderr()).isZero();
    }
}
