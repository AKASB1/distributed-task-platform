package io.akasb.taskplatform.worker;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.UUID;

/** What a {@link TaskHandler} sees of its job and attempt. */
public interface TaskContext {

    UUID jobId();

    /** 1-based attempt number. */
    int attempt();

    String queue();

    /** The job payload (a JSON object). */
    JsonNode payload();

    /** Reports progress in percent (0-100); sent with the next heartbeat. */
    void reportProgress(int percent);

    /** True once the attempt was cancelled, timed out, or lost its lease. */
    boolean isCancelled();

    /** Throws {@link InterruptedException} if the attempt should stop. */
    default void checkCancelled() throws InterruptedException {
        if (isCancelled() || Thread.currentThread().isInterrupted()) throw new InterruptedException("attempt stopped");
    }
}
