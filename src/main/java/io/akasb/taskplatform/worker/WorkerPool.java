package io.akasb.taskplatform.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.akasb.taskplatform.dispatch.HeartbeatResult;
import io.akasb.taskplatform.dispatch.Outcome;
import io.akasb.taskplatform.domain.FailureKind;
import io.akasb.taskplatform.domain.Lease;
import io.akasb.taskplatform.domain.TaskFailure;
import io.akasb.taskplatform.observability.JobMdc;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * In-process worker pool for one queue: {@code concurrency} slots, each leasing one attempt at a time.
 *
 * <p>For every attempt the slot thread runs the handler on a virtual thread and supervises it: it renews the lease
 * every {@code heartbeatInterval} (sending progress), enforces the job's execution timeout, and interrupts the
 * handler when the job is cancelled or the lease is lost. Deadlines are measured with the injected {@link Clock};
 * the real-time waits between checks are bounded by the heartbeat interval.
 */
public final class WorkerPool implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);

    public record Settings(String name, String queue, int concurrency, Duration heartbeatInterval,
                           Duration pollTimeout, Duration shutdownGrace) {
        public Settings {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(queue, "queue");
            Objects.requireNonNull(heartbeatInterval, "heartbeatInterval");
            Objects.requireNonNull(pollTimeout, "pollTimeout");
            Objects.requireNonNull(shutdownGrace, "shutdownGrace");
            if (concurrency < 1) throw new IllegalArgumentException("concurrency must be >= 1");
            if (heartbeatInterval.isNegative() || heartbeatInterval.isZero()) {
                throw new IllegalArgumentException("heartbeatInterval must be positive");
            }
            if (pollTimeout.isNegative() || pollTimeout.isZero()) {
                throw new IllegalArgumentException("pollTimeout must be positive");
            }
            if (shutdownGrace.isNegative()) throw new IllegalArgumentException("shutdownGrace must not be negative");
        }
    }

    private final Settings settings;
    private final WorkerProtocol protocol;
    private final TaskHandlerRegistry handlers;
    private final Clock clock;
    private final ObjectMapper mapper;
    private final String instanceId = UUID.randomUUID().toString().substring(0, 8);
    private final AtomicInteger busy = new AtomicInteger();
    private final AtomicLong busyNanos = new AtomicLong();
    private final List<Slot> slots = new ArrayList<>();
    private final ExecutorService handlerExecutor;
    private volatile boolean running;
    private volatile boolean killed;
    private volatile boolean stopped;
    private long stopDeadlineNanos;

    public WorkerPool(Settings settings, WorkerProtocol protocol, TaskHandlerRegistry handlers, Clock clock,
                      ObjectMapper mapper) {
        this.settings = Objects.requireNonNull(settings);
        this.protocol = Objects.requireNonNull(protocol);
        this.handlers = Objects.requireNonNull(handlers);
        this.clock = Objects.requireNonNull(clock);
        this.mapper = Objects.requireNonNull(mapper);
        this.handlerExecutor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name(settings.name() + "-handler-", 0).factory());
    }

    public synchronized void start() {
        if (running || killed || stopped) throw new IllegalStateException("a pool can only be started once");
        running = true;
        for (int i = 0; i < settings.concurrency(); i++) {
            Slot slot = new Slot(settings.name() + "@" + instanceId + "-" + i);
            slot.thread = Thread.ofPlatform().name(settings.name() + "-slot-" + i).daemon(true).unstarted(slot);
            slots.add(slot);
            slot.thread.start();
        }
        log.info("worker pool {} started: queue={} concurrency={}", settings.name(), settings.queue(),
                settings.concurrency());
    }

    /**
     * Graceful stop: idle slots stop at once; running attempts get {@code shutdownGrace} to finish, then they are
     * interrupted and reported as retryable failures.
     */
    public void stop() {
        beginStop();
        awaitStop();
    }

    /** First half of {@link #stop()}: stop taking new work and wake idle slots; returns immediately. */
    public synchronized void beginStop() {
        if (!running) return;
        running = false;
        stopped = true;
        stopDeadlineNanos = System.nanoTime() + settings.shutdownGrace().toNanos();
        slots.forEach(Slot::interruptIfIdle);
    }

    /** Second half of {@link #stop()}: wait for running attempts until the grace period ends, then interrupt them. */
    public synchronized void awaitStop() {
        if (!stopped || handlerExecutor.isShutdown()) return;
        for (Slot s : slots) joinUntil(s.thread, stopDeadlineNanos);
        slots.stream().filter(s -> s.thread.isAlive()).forEach(s -> s.thread.interrupt());
        for (Slot s : slots) joinUntil(s.thread, System.nanoTime() + TimeUnit.SECONDS.toNanos(5));
        handlerExecutor.shutdownNow();
        log.info("worker pool {} stopped", settings.name());
    }

    /**
     * Simulates a crash for failure-injection tests: slots and handlers stop immediately and nothing more is
     * reported to the control plane, so held leases simply expire.
     */
    public synchronized void kill() {
        killed = true;
        running = false;
        slots.forEach(s -> s.thread.interrupt());
        handlerExecutor.shutdownNow();
        log.warn("worker pool {} killed", settings.name());
    }

    @Override
    public void close() {
        stop();
    }

    public String name() { return settings.name(); }
    public String queue() { return settings.queue(); }
    public int concurrency() { return settings.concurrency(); }
    public int busy() { return busy.get(); }
    /** Fraction of slots currently executing an attempt. */
    public double utilization() { return (double) busy.get() / settings.concurrency(); }
    /** Total time slots spent executing attempts, in nanoseconds. */
    public long busyNanos() { return busyNanos.get(); }

    // ------------------------------------------------------------------------------------------ slot

    private final class Slot implements Runnable {
        final String workerId;
        private final Object stateLock = new Object();
        private boolean executing; // guarded by stateLock
        Thread thread;

        Slot(String workerId) {
            this.workerId = workerId;
        }

        /** Wakes a slot that is waiting for work; a slot executing an attempt keeps its shutdown grace period. */
        void interruptIfIdle() {
            synchronized (stateLock) {
                if (!executing) thread.interrupt();
            }
        }

        @Override
        public void run() {
            while (running && !killed) {
                Optional<Lease> lease;
                try {
                    lease = protocol.acquireNext(workerId, settings.queue(), settings.pollTimeout());
                } catch (InterruptedException e) {
                    break;
                } catch (RuntimeException e) {
                    log.warn("worker {} could not acquire work: {}", workerId, e.toString());
                    LockSupport.parkNanos(settings.pollTimeout().toNanos());
                    continue;
                }
                if (lease.isEmpty()) continue;
                if (killed) return; // crashed right after leasing: the lease will expire
                synchronized (stateLock) {
                    executing = true;
                    // stop() may have interrupted this slot as idle while the lease was being claimed; the attempt
                    // is running now and gets the grace period like any other.
                    Thread.interrupted();
                }
                busy.incrementAndGet();
                long started = System.nanoTime();
                try {
                    execute(workerId, lease.get());
                } catch (RuntimeException e) {
                    log.error("worker {} failed while supervising an attempt", workerId, e);
                } finally {
                    busyNanos.addAndGet(System.nanoTime() - started);
                    busy.decrementAndGet();
                    synchronized (stateLock) {
                        executing = false;
                    }
                }
                if (Thread.currentThread().isInterrupted() && !running) break;
            }
        }
    }

    private void execute(String workerId, Lease lease) {
        try (var mdc = JobMdc.of(lease.job().id(), lease.attempt(), lease.job().queue(), workerId)) {
            Optional<TaskHandler> handler = handlers.find(lease.job().type());
            if (handler.isEmpty()) {
                report(() -> protocol.fail(lease, new TaskFailure(FailureKind.NON_RETRYABLE,
                        "no handler for type " + lease.job().type())));
                return;
            }
            AttemptContext context;
            try {
                context = new AttemptContext(lease, mapper.readTree(lease.job().payload()));
            } catch (JsonProcessingException e) {
                report(() -> protocol.fail(lease, new TaskFailure(FailureKind.NON_RETRYABLE, "invalid payload")));
                return;
            }
            log.info("attempt started type={}", lease.job().type());
            Map<String, String> mdcCopy = MDC.getCopyOfContextMap();
            Future<Object> future = handlerExecutor.submit(() -> {
                if (mdcCopy != null) MDC.setContextMap(mdcCopy);
                try {
                    return handler.get().handle(context);
                } finally {
                    MDC.clear();
                }
            });
            supervise(lease, context, future);
        }
    }

    private void supervise(Lease initial, AttemptContext context, Future<Object> future) {
        Lease lease = initial;
        Instant deadline = lease.delivery().startedAt().plus(lease.job().timeout());
        Instant nextHeartbeat = clock.instant().plus(settings.heartbeatInterval());
        while (true) {
            if (killed) {
                future.cancel(true);
                return;
            }
            Instant now = clock.instant();
            Instant nextCheck = nextHeartbeat.isBefore(deadline) ? nextHeartbeat : deadline;
            long waitNanos = Math.min(settings.heartbeatInterval().toNanos(),
                    Math.max(TimeUnit.MILLISECONDS.toNanos(1), Duration.between(now, nextCheck).toNanos()));
            try {
                Object result = future.get(waitNanos, TimeUnit.NANOSECONDS);
                if (killed) return;
                reportSuccess(lease, result);
                return;
            } catch (TimeoutException e) {
                now = clock.instant();
                if (!now.isBefore(deadline)) {
                    stopHandler(context, future);
                    final Lease timedOut = lease;
                    if (!killed) report(() -> protocol.fail(timedOut, new TaskFailure(FailureKind.TIMEOUT,
                            "exceeded timeout of " + timedOut.job().timeout().toMillis() + " ms")));
                    return;
                }
                if (!now.isBefore(nextHeartbeat)) {
                    HeartbeatResult hb;
                    try {
                        hb = protocol.heartbeat(lease, context.progress());
                    } catch (RuntimeException ex) {
                        // Keep running; if the control plane stays unreachable the lease expires and the job is
                        // retried elsewhere, and our late report will be rejected.
                        log.warn("heartbeat failed: {}", ex.toString());
                        nextHeartbeat = now.plus(settings.heartbeatInterval());
                        continue;
                    }
                    switch (hb.status()) {
                        case RENEWED -> {
                            lease = hb.lease();
                            nextHeartbeat = now.plus(settings.heartbeatInterval());
                        }
                        case CANCELLED -> {
                            stopHandler(context, future);
                            final Lease cancelled = lease;
                            report(() -> {
                                protocol.acknowledgeCancel(cancelled);
                                return Outcome.CANCELLED;
                            });
                            return;
                        }
                        case LOST -> {
                            stopHandler(context, future);
                            log.warn("lease lost; attempt abandoned");
                            return;
                        }
                    }
                }
            } catch (ExecutionException e) {
                if (killed) return;
                TaskFailure failure = classify(e.getCause());
                final Lease failed = lease;
                report(() -> protocol.fail(failed, failure));
                return;
            } catch (CancellationException e) {
                return;
            } catch (InterruptedException e) {
                // The pool is stopping and the grace period is over.
                stopHandler(context, future);
                final Lease interrupted = lease;
                if (!killed) report(() -> protocol.fail(interrupted,
                        new TaskFailure(FailureKind.WORKER_SHUTDOWN, "worker pool stopped during the attempt")));
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static void stopHandler(AttemptContext context, Future<Object> future) {
        context.cancelled = true;
        future.cancel(true);
    }

    private void report(java.util.function.Supplier<Outcome> call) {
        if (killed) return;
        try {
            Outcome outcome = call.get();
            if (outcome != Outcome.ACCEPTED) log.warn("report not accepted: {}", outcome);
        } catch (RuntimeException e) {
            // The lease will expire and the reconciler retries the job.
            log.error("could not report attempt outcome: {}", e.toString());
        }
    }

    /**
     * Reports a successful attempt. A result that cannot be serialised fails the attempt permanently instead of being
     * stored as null; if storing the result fails, the attempt is reported as a retryable failure so it is not left
     * open until its lease expires (if that report fails too, the lease expires and the job is retried).
     */
    private void reportSuccess(Lease lease, Object result) {
        String json;
        try {
            json = toJson(result);
        } catch (JsonProcessingException | IllegalArgumentException e) {
            report(() -> protocol.fail(lease, new TaskFailure(FailureKind.NON_RETRYABLE,
                    "handler result is not JSON-serialisable: " + e.getClass().getSimpleName())));
            return;
        }
        Outcome outcome;
        try {
            outcome = protocol.complete(lease, json);
        } catch (RuntimeException e) {
            log.error("could not store the attempt result: {}", e.toString());
            report(() -> protocol.fail(lease, new TaskFailure(FailureKind.RETRYABLE,
                    "the result could not be stored: " + e.getClass().getSimpleName())));
            return;
        }
        if (outcome != Outcome.ACCEPTED) log.warn("report not accepted: {}", outcome);
    }

    /** Serialises a handler result; U+0000 in strings is replaced because PostgreSQL jsonb cannot store it. */
    private String toJson(Object result) throws JsonProcessingException {
        if (result == null) return null;
        JsonNode tree = result instanceof JsonNode node ? node : mapper.valueToTree(result);
        return mapper.writeValueAsString(withoutNul(tree));
    }

    private JsonNode withoutNul(JsonNode node) {
        if (node.isTextual()) {
            String text = node.textValue();
            return text.indexOf('\u0000') < 0 ? node : mapper.getNodeFactory().textNode(text.replace('\u0000', '\uFFFD'));
        }
        if (node.isArray()) {
            var copy = mapper.createArrayNode();
            node.forEach(child -> copy.add(withoutNul(child)));
            return copy;
        }
        if (node.isObject()) {
            var copy = mapper.createObjectNode();
            node.properties().forEach(e -> copy.set(e.getKey().replace('\u0000', '\uFFFD'), withoutNul(e.getValue())));
            return copy;
        }
        return node;
    }

    static TaskFailure classify(Throwable error) {
        if (error instanceof NonRetryableTaskException) {
            return new TaskFailure(FailureKind.NON_RETRYABLE, error.getMessage());
        }
        String message = error.getMessage() == null ? error.getClass().getSimpleName()
                : error.getClass().getSimpleName() + ": " + error.getMessage();
        return new TaskFailure(FailureKind.RETRYABLE, message);
    }

    private static void joinUntil(Thread thread, long deadlineNanos) {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0) return;
        try {
            thread.join(Duration.ofNanos(remaining));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** {@link TaskContext} for one attempt. */
    private static final class AttemptContext implements TaskContext {
        private final Lease lease;
        private final JsonNode payload;
        private final AtomicInteger progress = new AtomicInteger(-1);
        volatile boolean cancelled;

        AttemptContext(Lease lease, JsonNode payload) {
            this.lease = lease;
            this.payload = payload;
        }

        @Override public UUID jobId() { return lease.job().id(); }
        @Override public int attempt() { return lease.attempt(); }
        @Override public String queue() { return lease.job().queue(); }
        @Override public JsonNode payload() { return payload; }
        @Override public boolean isCancelled() { return cancelled; }

        @Override
        public void reportProgress(int percent) {
            progress.set(Math.max(0, Math.min(100, percent)));
        }

        Integer progress() {
            int p = progress.get();
            return p < 0 ? null : p;
        }
    }
}
