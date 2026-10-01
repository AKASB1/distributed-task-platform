package io.akasb.taskplatform.config;

import io.akasb.taskplatform.dispatch.Reconciler;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Runs startup recovery before the worker pools and the web server start, then a reconciler pass every
 * {@code taskplatform.reconciler.interval}.
 */
public class ReconcilerRunner implements SmartLifecycle {
    static final int PHASE = 100;
    private static final Logger log = LoggerFactory.getLogger(ReconcilerRunner.class);

    private final Reconciler reconciler;
    private final TaskPlatformProperties.Reconciler settings;
    private ScheduledExecutorService scheduler;
    private volatile boolean running;

    public ReconcilerRunner(Reconciler reconciler, TaskPlatformProperties.Reconciler settings) {
        this.reconciler = reconciler;
        this.settings = settings;
    }

    @Override
    public synchronized void start() {
        running = true;
        if (!settings.isEnabled()) {
            log.info("reconciler disabled");
            return;
        }
        reconciler.recoverOnStartup();
        scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("reconciler").daemon(true)
                .factory());
        long interval = settings.getInterval().toMillis();
        scheduler.scheduleWithFixedDelay(this::pass, interval, interval, TimeUnit.MILLISECONDS);
    }

    private void pass() {
        try {
            reconciler.reconcileOnce();
        } catch (VirtualMachineError e) {
            log.error("reconciler pass failed fatally", e);
            throw e;
        } catch (Throwable e) {
            // a scheduled task that throws is never run again, so log and keep the schedule alive
            log.error("reconciler pass failed", e);
        }
    }

    @Override
    public synchronized void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdownNow();
            try {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            scheduler = null;
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
