package io.akasb.taskplatform.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1")
public class JobController {
    public static final String IDEMPOTENCY_KEY = "Idempotency-Key";
    public static final String IDEMPOTENT_REPLAYED = "Idempotent-Replayed";

    private final JobService service;
    private final ObjectMapper mapper;

    public JobController(JobService service, ObjectMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    /** 201 for a new job; 200 with {@code Idempotent-Replayed: true} when the key was already used. */
    @PostMapping("/jobs")
    public ResponseEntity<JobResponse> submit(@Valid @RequestBody SubmitJobRequest request,
                                              @RequestHeader(value = IDEMPOTENCY_KEY, required = false) String key) {
        JobService.SubmitOutcome outcome = service.submit(request, key);
        if (outcome.created()) {
            return ResponseEntity.created(URI.create("/v1/jobs/" + outcome.job().id()))
                    .body(JobResponse.of(outcome.job(), List.of(), mapper));
        }
        JobService.JobDetails details = service.get(outcome.job().id());
        return ResponseEntity.status(HttpStatus.OK)
                .header(IDEMPOTENT_REPLAYED, "true")
                .location(URI.create("/v1/jobs/" + outcome.job().id()))
                .body(JobResponse.of(details.job(), details.deliveries(), mapper));
    }

    @GetMapping("/jobs/{id}")
    public JobResponse get(@PathVariable UUID id) {
        JobService.JobDetails details = service.get(id);
        return JobResponse.of(details.job(), details.deliveries(), mapper);
    }

    @PostMapping("/jobs/{id}/cancel")
    public JobResponse cancel(@PathVariable UUID id) {
        JobService.JobDetails details = service.cancel(id);
        return JobResponse.of(details.job(), details.deliveries(), mapper);
    }

    @GetMapping("/queues/{name}/stats")
    public QueueStatsResponse stats(@PathVariable String name) {
        return QueueStatsResponse.of(service.queueStats(name));
    }
}
