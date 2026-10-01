package io.akasb.taskplatform.api;

import io.akasb.taskplatform.persistence.QueueStats;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** JSON view of {@code GET /v1/queues/{name}/stats}. */
public record QueueStatsResponse(String queue, Map<String, Long> counts, long total, Instant oldestQueuedAt,
                                 long oldestQueuedAgeMs, Instant asOf) {

    public static QueueStatsResponse of(QueueStats stats) {
        Map<String, Long> counts = new LinkedHashMap<>();
        stats.counts().forEach((state, n) -> counts.put(state.name(), n));
        return new QueueStatsResponse(stats.queue(), counts, stats.total(), stats.oldestQueuedReadyAt(),
                stats.oldestQueuedAge().toMillis(), stats.now());
    }
}
