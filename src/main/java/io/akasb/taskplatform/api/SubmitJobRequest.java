package io.akasb.taskplatform.api;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/**
 * Body of {@code POST /v1/jobs}. Only {@code type} is required.
 *
 * @param queue       defaults to {@code "default"}
 * @param payload     a JSON object handed to the handler; defaults to {@code {}}
 * @param maxAttempts total attempts including the first; defaults to the configured value
 * @param timeoutMs   execution timeout per attempt; defaults to the configured value
 */
public record SubmitJobRequest(
        @NotBlank @Size(max = 64) String type,
        @Pattern(regexp = JobService.QUEUE_NAME_PATTERN, message = "must match " + JobService.QUEUE_NAME_PATTERN)
        String queue,
        JsonNode payload,
        @Min(1) @Max(100) Integer maxAttempts,
        @Min(1) Long timeoutMs) {
}
