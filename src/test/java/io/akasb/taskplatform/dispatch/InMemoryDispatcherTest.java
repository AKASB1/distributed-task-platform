package io.akasb.taskplatform.dispatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Per-queue FIFO with de-duplication of waiting ids: ordering, isolation, duplicates, timeouts, depth, and a
 * multi-producer/multi-consumer run in which every id must be delivered exactly once.
 */
class InMemoryDispatcherTest {
    private final InMemoryDispatcher dispatcher = new InMemoryDispatcher();
    private ExecutorService pool;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (pool != null) {
            pool.shutdownNow();
            assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
        dispatcher.close();
    }

    private static UUID id(long n) {
        return new UUID(0xD15BA7C4L, n);
    }

    private Optional<UUID> pollNow(String queue) throws InterruptedException {
        return dispatcher.poll(queue, Duration.ZERO);
    }

    // ----------------------------------------------------------------------------- single thread

    @Test
    void deliversInFifoOrderPerQueue() throws InterruptedException {
        for (int i = 0; i < 100; i++) dispatcher.dispatch("q", id(i));
        for (int i = 0; i < 100; i++) assertThat(pollNow("q")).contains(id(i));
        assertThat(pollNow("q")).isEmpty();
    }

    @Test
    void queuesAreIsolated() throws InterruptedException {
        dispatcher.dispatch("a", id(1));
        dispatcher.dispatch("b", id(2));
        dispatcher.dispatch("a", id(3));

        assertThat(dispatcher.depth("a")).isEqualTo(2);
        assertThat(dispatcher.depth("b")).isEqualTo(1);
        assertThat(pollNow("b")).contains(id(2));
        assertThat(pollNow("b")).isEmpty();
        assertThat(pollNow("c")).isEmpty();
        assertThat(pollNow("a")).contains(id(1));
        assertThat(pollNow("a")).contains(id(3));
    }

    @Test
    void theSameIdMayWaitInTwoDifferentQueues() throws InterruptedException {
        dispatcher.dispatch("a", id(1));
        dispatcher.dispatch("b", id(1));
        assertThat(pollNow("a")).contains(id(1));
        assertThat(pollNow("b")).contains(id(1));
    }

    @Test
    void aWaitingIdIsNotEnqueuedTwice() throws InterruptedException {
        dispatcher.dispatch("q", id(1));
        dispatcher.dispatch("q", id(2));
        dispatcher.dispatch("q", id(1));
        dispatcher.dispatch("q", id(1));

        assertThat(dispatcher.depth("q")).isEqualTo(2);
        assertThat(pollNow("q")).contains(id(1));
        assertThat(pollNow("q")).contains(id(2));
        assertThat(pollNow("q")).isEmpty();
    }

    @Test
    void anIdCanBeReEnqueuedAfterItWasPolled() throws InterruptedException {
        dispatcher.dispatch("q", id(1));
        assertThat(pollNow("q")).contains(id(1));

        dispatcher.dispatch("q", id(1));
        assertThat(dispatcher.depth("q")).isEqualTo(1);
        assertThat(pollNow("q")).contains(id(1));

        dispatcher.dispatch("q", id(2));
        dispatcher.dispatch("q", id(1));
        assertThat(pollNow("q")).as("re-enqueued id goes to the back").contains(id(2));
        assertThat(pollNow("q")).contains(id(1));
        assertThat(pollNow("q")).isEmpty();
    }

    @Test
    void depthCountsWaitingIds() throws InterruptedException {
        assertThat(dispatcher.depth("unknown")).isZero();
        dispatcher.dispatch("q", id(1));
        dispatcher.dispatch("q", id(2));
        dispatcher.dispatch("q", id(3));
        assertThat(dispatcher.depth("q")).isEqualTo(3);
        pollNow("q");
        assertThat(dispatcher.depth("q")).isEqualTo(2);
        pollNow("q");
        pollNow("q");
        assertThat(dispatcher.depth("q")).isZero();
    }

    @Test
    void isNotDurable() {
        assertThat(dispatcher.durable()).isFalse();
    }

    @Test
    void pollOnAnEmptyQueueReturnsEmptyAfterTheTimeout() throws InterruptedException {
        Duration timeout = Duration.ofMillis(30);
        long start = System.nanoTime();
        Optional<UUID> got = dispatcher.poll("empty", timeout);
        long elapsed = System.nanoTime() - start;

        assertThat(got).isEmpty();
        assertThat(elapsed).as("waited at least the timeout").isGreaterThanOrEqualTo(timeout.toNanos());
        assertThat(elapsed).as("but not much longer").isLessThan(TimeUnit.SECONDS.toNanos(5));
    }

    @Test
    void zeroAndNegativeTimeoutsDoNotBlock() throws InterruptedException {
        long start = System.nanoTime();
        assertThat(dispatcher.poll("empty", Duration.ZERO)).isEmpty();
        assertThat(dispatcher.poll("empty", Duration.ofSeconds(-10))).isEmpty();
        assertThat(System.nanoTime() - start).isLessThan(TimeUnit.SECONDS.toNanos(2));
    }

    @Test
    void pollOfAnInterruptedThreadThrows() {
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> dispatcher.poll("q", Duration.ofSeconds(5)))
                    .isInstanceOf(InterruptedException.class);
        } finally {
            Thread.interrupted(); // never leak the flag into other tests
        }
    }

    // ---------------------------------------------------------------------------------- threads

    @Test
    @Timeout(20)
    void aBlockedPollReceivesAnIdDispatchedLater() throws Exception {
        pool = Executors.newSingleThreadExecutor();
        CountDownLatch polling = new CountDownLatch(1);
        Future<Optional<UUID>> result = pool.submit(() -> {
            polling.countDown();
            return dispatcher.poll("late", Duration.ofSeconds(10));
        });
        assertThat(polling.await(5, TimeUnit.SECONDS)).isTrue();

        dispatcher.dispatch("late", id(42));

        assertThat(result.get(10, TimeUnit.SECONDS)).contains(id(42));
        assertThat(dispatcher.depth("late")).isZero();
    }

    @Test
    @Timeout(30)
    void concurrentDuplicateDispatchesEnqueueEachIdOnce() throws Exception {
        int threads = 4;
        int ids = 1_000;
        pool = Executors.newFixedThreadPool(threads);
        CyclicBarrier start = new CyclicBarrier(threads);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            futures.add(pool.submit(() -> {
                start.await(10, TimeUnit.SECONDS);
                for (int i = 0; i < ids; i++) dispatcher.dispatch("dup", id(i));
                return null;
            }));
        }
        for (Future<?> f : futures) f.get(20, TimeUnit.SECONDS);

        assertThat(dispatcher.depth("dup")).isEqualTo(ids);
        Set<UUID> seen = new HashSet<>();
        Optional<UUID> next;
        while ((next = pollNow("dup")).isPresent()) assertThat(seen.add(next.get())).as("duplicate").isTrue();
        assertThat(seen).hasSize(ids);
    }

    @Test
    @Timeout(60)
    void fourProducersAndFourConsumersDeliverEveryIdExactlyOnce() throws Exception {
        int producers = 4;
        int perProducer = 2_500;
        int consumers = 4;
        int total = producers * perProducer;
        pool = Executors.newFixedThreadPool(producers + consumers);
        CyclicBarrier start = new CyclicBarrier(producers + consumers);
        AtomicInteger received = new AtomicInteger();
        Map<UUID, AtomicInteger> deliveries = new ConcurrentHashMap<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);

        List<Future<?>> producerFutures = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            final long producer = p;
            producerFutures.add(pool.submit(() -> {
                start.await(10, TimeUnit.SECONDS);
                for (int i = 0; i < perProducer; i++) dispatcher.dispatch("load", new UUID(producer, i));
                return null;
            }));
        }
        List<Future<List<UUID>>> consumerFutures = new ArrayList<>();
        for (int c = 0; c < consumers; c++) {
            consumerFutures.add(pool.submit(() -> {
                List<UUID> mine = new ArrayList<>();
                start.await(10, TimeUnit.SECONDS);
                while (received.get() < total && System.nanoTime() < deadline) {
                    Optional<UUID> got = dispatcher.poll("load", Duration.ofMillis(20));
                    if (got.isEmpty()) continue;
                    mine.add(got.get());
                    deliveries.computeIfAbsent(got.get(), k -> new AtomicInteger()).incrementAndGet();
                    received.incrementAndGet();
                }
                return mine;
            }));
        }

        for (Future<?> f : producerFutures) f.get(30, TimeUnit.SECONDS);
        List<List<UUID>> perConsumer = new ArrayList<>();
        for (Future<List<UUID>> f : consumerFutures) perConsumer.add(f.get(40, TimeUnit.SECONDS));

        assertThat(received.get()).as("ids received before the deadline").isEqualTo(total);
        assertThat(deliveries).hasSize(total);
        assertThat(deliveries.values()).allSatisfy(n -> assertThat(n.get()).isEqualTo(1));
        Set<UUID> expected = new HashSet<>();
        for (long p = 0; p < producers; p++) {
            for (int i = 0; i < perProducer; i++) expected.add(new UUID(p, i));
        }
        assertThat(deliveries.keySet()).isEqualTo(expected);
        assertThat(perConsumer.stream().mapToInt(List::size).sum()).isEqualTo(total);
        assertThat(dispatcher.depth("load")).isZero();
        assertThat(pollNow("load")).isEmpty();

        // FIFO survives concurrency: each consumer sees any one producer's ids in dispatch order.
        for (List<UUID> mine : perConsumer) {
            Map<Long, Long> lastSeen = new HashMap<>();
            for (UUID u : mine) {
                Long previous = lastSeen.put(u.getMostSignificantBits(), u.getLeastSignificantBits());
                if (previous != null) assertThat(u.getLeastSignificantBits()).isGreaterThan(previous);
            }
        }

        // Every waiting marker was cleared: all ids can be enqueued again.
        for (UUID u : expected) dispatcher.dispatch("load", u);
        assertThat(dispatcher.depth("load")).isEqualTo(total);
    }
}
