package io.akasb.taskplatform.persistence;

import io.akasb.taskplatform.domain.JobState;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

/**
 * Per-queue counts and the age of the oldest QUEUED job.
 *
 * @param oldestQueuedReadyAt null when nothing is queued
 */
public record QueueStats(String queue, Map<JobState, Long> counts, Instant oldestQueuedReadyAt, Instant now) {

    public QueueStats {
        EnumMap<JobState, Long> all = new EnumMap<>(JobState.class);
        for (JobState s : JobState.values()) all.put(s, counts.getOrDefault(s, 0L));
        counts = Collections.unmodifiableMap(all);
    }

    /** Age of the oldest QUEUED job, or {@link Duration#ZERO} when nothing is queued. */
    public Duration oldestQueuedAge() {
        if (oldestQueuedReadyAt == null || now.isBefore(oldestQueuedReadyAt)) return Duration.ZERO;
        return Duration.between(oldestQueuedReadyAt, now);
    }

    public long total() {
        return counts.values().stream().mapToLong(Long::longValue).sum();
    }
}
