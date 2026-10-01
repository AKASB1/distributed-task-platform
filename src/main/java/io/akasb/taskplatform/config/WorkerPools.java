package io.akasb.taskplatform.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.akasb.taskplatform.dispatch.JobDispatcher;
import io.akasb.taskplatform.worker.TaskHandlerRegistry;
import io.akasb.taskplatform.worker.WorkerPool;
import io.akasb.taskplatform.worker.WorkerProtocol;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Creates one {@link WorkerPool} per configured pool, registers its utilization gauges, starts the pools after
 * startup recovery and stops them gracefully before the reconciler.
 */
public class WorkerPools implements SmartLifecycle {
    static final int PHASE = ReconcilerRunner.PHASE + 100;
    private static final Logger log = LoggerFactory.getLogger(WorkerPools.class);

    private final List<WorkerPool> pools = new ArrayList<>();
    private final boolean enabled;
    private volatile boolean running;

    public WorkerPools(TaskPlatformProperties props, WorkerProtocol protocol, TaskHandlerRegistry handlers,
                       Clock clock, ObjectMapper mapper, MeterRegistry registry, JobDispatcher dispatcher) {
        TaskPlatformProperties.Workers w = props.getWorkers();
        this.enabled = w.isEnabled();
        Set<String> names = new HashSet<>();
        for (TaskPlatformProperties.Pool p : w.getPools()) {
            if (p.getName() == null || p.getQueue() == null) throw new IllegalArgumentException("pool needs name and queue");
            if (!names.add(p.getName())) throw new IllegalArgumentException("duplicate pool name " + p.getName());
            WorkerPool pool = new WorkerPool(new WorkerPool.Settings(p.getName(), p.getQueue(), p.getConcurrency(),
                    props.getHeartbeatInterval(), w.getPollTimeout(), w.getShutdownGrace()), protocol, handlers,
                    clock, mapper);
            pools.add(pool);
            bindMetrics(pool, registry, dispatcher);
        }
    }

    private static void bindMetrics(WorkerPool pool, MeterRegistry registry, JobDispatcher dispatcher) {
        Gauge.builder("taskplatform.worker.utilization", pool, WorkerPool::utilization)
                .description("Fraction of worker slots executing an attempt")
                .tag("pool", pool.name()).tag("queue", pool.queue()).register(registry);
        Gauge.builder("taskplatform.worker.busy", pool, WorkerPool::busy)
                .description("Worker slots executing an attempt").tag("pool", pool.name()).register(registry);
        Gauge.builder("taskplatform.worker.slots", pool, WorkerPool::concurrency)
                .description("Configured worker slots").tag("pool", pool.name()).register(registry);
        FunctionCounter.builder("taskplatform.worker.busy.time", pool,
                        p -> p.busyNanos() / (double) TimeUnit.SECONDS.toNanos(1))
                .baseUnit("seconds").description("Cumulative slot time spent executing attempts")
                .tag("pool", pool.name()).register(registry);
        Gauge.builder("taskplatform.queue.depth", dispatcher, d -> d.depth(pool.queue()))
                .description("Messages waiting in the dispatcher (-1 if unknown)")
                .tag("queue", pool.queue()).register(registry);
    }

    public List<WorkerPool> pools() {
        return List.copyOf(pools);
    }

    @Override
    public synchronized void start() {
        running = true;
        if (!enabled) {
            log.info("worker pools disabled");
            return;
        }
        pools.forEach(WorkerPool::start);
    }

    @Override
    public synchronized void stop() {
        running = false;
        if (enabled) {
            // stop taking work everywhere first, then wait: pools share one grace period instead of one each
            pools.forEach(WorkerPool::beginStop);
            pools.forEach(WorkerPool::awaitStop);
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }
}
