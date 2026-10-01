package io.akasb.taskplatform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.akasb.taskplatform.api.IdempotencyConflictException;
import io.akasb.taskplatform.api.InvalidJobRequestException;
import io.akasb.taskplatform.api.JobService;
import io.akasb.taskplatform.api.SubmitJobRequest;
import io.akasb.taskplatform.dispatch.Checkpoints;
import io.akasb.taskplatform.dispatch.InMemoryDispatcher;
import io.akasb.taskplatform.dispatch.JobLifecycle;
import io.akasb.taskplatform.domain.JobState;
import io.akasb.taskplatform.domain.RetryPolicy;
import io.akasb.taskplatform.observability.LifecycleListener;
import io.akasb.taskplatform.persistence.InMemoryJobRepository;
import io.akasb.taskplatform.support.MutableClock;
import java.time.Duration;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The scaffold's original test, kept in spirit (submit → QUEUED → the id is on the local queue; a duplicate is not
 * created twice) and adapted to server-generated ids and idempotency keys.
 */
class JobServiceTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private InMemoryDispatcher dispatcher;
    private InMemoryJobRepository repository;
    private JobService service;

    @BeforeEach
    void setUp() {
        dispatcher = new InMemoryDispatcher();
        repository = new InMemoryJobRepository();
        MutableClock clock = MutableClock.startingAt("2026-01-01T00:00:00Z");
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(1), 2.0, Duration.ofMinutes(1), 0.5, new Random(1));
        JobLifecycle lifecycle = new JobLifecycle(repository, dispatcher, retry, clock, Duration.ofSeconds(30),
                LifecycleListener.none(), Checkpoints.none());
        service = new JobService(lifecycle, repository, () -> Set.of("sleep", "flaky"), () -> Set.of("default"),
                new JobService.Limits(3, 20, Duration.ofSeconds(30), Duration.ofHours(1), 1024), mapper);
    }

    private SubmitJobRequest sleep(long ms) {
        return new SubmitJobRequest("sleep", null, mapper.createObjectNode().put("durationMs", ms), null, null);
    }

    @Test
    void submitsToLocalQueue() throws InterruptedException {
        JobService.SubmitOutcome outcome = service.submit(sleep(10), "one");
        assertThat(outcome.created()).isTrue();
        assertThat(outcome.job().state()).isEqualTo(JobState.QUEUED);
        assertThat(outcome.job().version()).isEqualTo(1);
        assertThat(outcome.job().maxAttempts()).isEqualTo(3);
        assertThat(outcome.job().timeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(dispatcher.poll("default", Duration.ZERO)).contains(outcome.job().id());

        JobService.SubmitOutcome again = service.submit(sleep(10), "one");
        assertThat(again.created()).isFalse();
        assertThat(again.job().id()).isEqualTo(outcome.job().id());
        assertThat(dispatcher.poll("default", Duration.ZERO)).isEmpty();
        assertThat(repository.countByState().values().stream().mapToLong(Long::longValue).sum()).isEqualTo(1);
    }

    @Test
    void sameKeyWithDifferentRequestIsRejected() {
        UUID first = service.submit(sleep(10), "k").job().id();
        assertThatThrownBy(() -> service.submit(sleep(11), "k"))
                .isInstanceOf(IdempotencyConflictException.class)
                .satisfies(e -> assertThat(((IdempotencyConflictException) e).existingJobId()).isEqualTo(first));
    }

    @Test
    void payloadKeyOrderDoesNotMatterForIdempotency() {
        var a = mapper.createObjectNode().put("durationMs", 5).put("x", "y");
        var b = mapper.createObjectNode().put("x", "y").put("durationMs", 5);
        UUID first = service.submit(new SubmitJobRequest("sleep", "default", a, 2, 1000L), "k2").job().id();
        assertThat(service.submit(new SubmitJobRequest("sleep", "default", b, 2, 1000L), "k2").job().id())
                .isEqualTo(first);
    }

    @Test
    void withoutKeyEverySubmissionCreatesAJob() {
        assertThat(service.submit(sleep(1), null).job().id()).isNotEqualTo(service.submit(sleep(1), null).job().id());
    }

    @Test
    void rejectsInvalidRequests() {
        assertThatThrownBy(() -> service.submit(new SubmitJobRequest("nope", null, null, null, null), null))
                .isInstanceOf(InvalidJobRequestException.class).hasMessageContaining("unknown job type");
        assertThatThrownBy(() -> service.submit(new SubmitJobRequest("sleep", "other", null, null, null), null))
                .isInstanceOf(InvalidJobRequestException.class).hasMessageContaining("no worker pool");
        assertThatThrownBy(() -> service.submit(new SubmitJobRequest("sleep", "Bad Queue", null, null, null), null))
                .isInstanceOf(InvalidJobRequestException.class).hasMessageContaining("queue name");
        assertThatThrownBy(() -> service.submit(new SubmitJobRequest("sleep", null, null, 21, null), null))
                .isInstanceOf(InvalidJobRequestException.class).hasMessageContaining("maxAttempts");
        assertThatThrownBy(() -> service.submit(new SubmitJobRequest("sleep", null, null, null, 3_600_001L), null))
                .isInstanceOf(InvalidJobRequestException.class).hasMessageContaining("timeoutMs");
        assertThatThrownBy(() -> service.submit(new SubmitJobRequest("sleep", null,
                mapper.createArrayNode(), null, null), null))
                .isInstanceOf(InvalidJobRequestException.class).hasMessageContaining("JSON object");
        assertThatThrownBy(() -> service.submit(new SubmitJobRequest("sleep", null,
                mapper.createObjectNode().put("big", "x".repeat(2000)), null, null), null))
                .isInstanceOf(InvalidJobRequestException.class).hasMessageContaining("exceeds");
        assertThatThrownBy(() -> service.submit(sleep(1), "has space"))
                .isInstanceOf(InvalidJobRequestException.class).hasMessageContaining("Idempotency-Key");
        assertThat(repository.countByState()).isEmpty();
    }

    @Test
    void getAndCancel() {
        UUID id = service.submit(sleep(1), null).job().id();
        assertThat(service.get(id).job().state()).isEqualTo(JobState.QUEUED);
        assertThat(service.cancel(id).job().state()).isEqualTo(JobState.CANCELLED);
        assertThat(service.queueStats("default").counts().get(JobState.CANCELLED)).isEqualTo(1);
        assertThat(Optional.ofNullable(service.queueStats("default").oldestQueuedReadyAt())).isEmpty();
    }
}
