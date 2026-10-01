package io.akasb.taskplatform.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.akasb.taskplatform.domain.Delivery;
import io.akasb.taskplatform.domain.Job;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** JSON view of a job and its deliveries (attempt history). */
public record JobResponse(
        UUID id,
        String queue,
        String type,
        String state,
        long version,
        int attempts,
        int maxAttempts,
        long timeoutMs,
        String idempotencyKey,
        JsonNode payload,
        JsonNode result,
        String lastError,
        Instant createdAt,
        Instant updatedAt,
        Instant readyAt,
        Instant nextAttemptAt,
        Instant finishedAt,
        List<DeliveryResponse> deliveries) {

    public record DeliveryResponse(
            int attempt,
            String workerId,
            String ackState,
            Instant leaseDeadline,
            Instant queuedAt,
            Instant startedAt,
            Instant heartbeatAt,
            Instant finishedAt,
            Integer progress,
            String error) {

        static DeliveryResponse of(Delivery d) {
            return new DeliveryResponse(d.attempt(), d.leaseOwner(), d.ackState().name(), d.leaseDeadline(),
                    d.queuedAt(), d.startedAt(), d.heartbeatAt(), d.finishedAt(), d.progress(), d.error());
        }
    }

    public static JobResponse of(Job job, List<Delivery> deliveries, ObjectMapper mapper) {
        return new JobResponse(job.id(), job.queue(), job.type(), job.state().name(), job.version(), job.attempts(),
                job.maxAttempts(), job.timeout().toMillis(), job.idempotencyKey(), json(job.payload(), mapper),
                json(job.result(), mapper), job.lastError(), job.createdAt(), job.updatedAt(), job.readyAt(),
                job.nextAttemptAt(), job.finishedAt(), deliveries.stream().map(DeliveryResponse::of).toList());
    }

    private static JsonNode json(String text, ObjectMapper mapper) {
        if (text == null) return null;
        try {
            return mapper.readTree(text);
        } catch (JsonProcessingException e) {
            return mapper.getNodeFactory().textNode(text);
        }
    }
}
