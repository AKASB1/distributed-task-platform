package io.akasb.taskplatform.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.akasb.taskplatform.dispatch.Checkpoints;
import io.akasb.taskplatform.dispatch.InMemoryDispatcher;
import io.akasb.taskplatform.dispatch.JobLifecycle;
import io.akasb.taskplatform.domain.RetryPolicy;
import io.akasb.taskplatform.observability.LifecycleListener;
import io.akasb.taskplatform.persistence.InMemoryJobRepository;
import io.akasb.taskplatform.support.MutableClock;
import java.time.Duration;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Payload content PostgreSQL jsonb cannot store faithfully is rejected up front (400, not 500 or a false 422), and
 * idempotent replays compare only what the request said, so a changed server default does not turn a replay into a
 * conflict.
 */
class JobServicePayloadRulesTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final InMemoryJobRepository repository = new InMemoryJobRepository();
    private final JobLifecycle lifecycle = new JobLifecycle(repository, new InMemoryDispatcher(),
            new RetryPolicy(Duration.ofSeconds(1), 2.0, Duration.ofMinutes(1), 0.5, new Random(1)),
            MutableClock.startingAt("2026-01-01T00:00:00Z"), Duration.ofSeconds(30), LifecycleListener.none(),
            Checkpoints.none());

    private JobService service(int defaultMaxAttempts, Duration defaultTimeout) {
        return new JobService(lifecycle, repository, () -> Set.of("sleep"), Set::of,
                new JobService.Limits(defaultMaxAttempts, 20, defaultTimeout, Duration.ofHours(1), 65_536), mapper);
    }

    private SubmitJobRequest withPayload(String json) throws Exception {
        return new SubmitJobRequest("sleep", null, mapper.readTree(json), null, null);
    }

    @Test
    void nulCharactersAndUnpairedSurrogatesAreRejected() {
        JobService service = service(3, Duration.ofSeconds(30));
        for (String json : new String[] {
                "{\"v\":\"a\\u0000b\"}", "{\"k\\u0000\":1}", "{\"a\":[{\"b\":\"\\u0000\"}]}",
                "{\"v\":\"\\ud83d\"}", "{\"v\":\"x\\ude00\"}", "{\"v\":\"\\ud83dx\"}"}) {
            assertThatThrownBy(() -> service.submit(withPayload(json), null)).as(json)
                    .isInstanceOf(InvalidJobRequestException.class).hasMessageContaining("NUL");
        }
        assertThat(repository.countByState()).isEmpty();
    }

    @Test
    void validSurrogatePairsAndOtherUnicodeAreAccepted() throws Exception {
        JobService service = service(3, Duration.ofSeconds(30));
        UUID id = service.submit(withPayload("{\"emoji\":\"\\ud83d\\ude00\",\"text\":\"\u4efb\u52a1\"}"), null).job().id();
        assertThat(mapper.readTree(repository.find(id).orElseThrow().payload()).path("emoji").asText())
                .isEqualTo("\uD83D\uDE00");
    }

    @Test
    void replayStillMatchesAfterAServerDefaultChanged() throws Exception {
        UUID id = service(3, Duration.ofSeconds(30)).submit(withPayload("{\"durationMs\":5}"), "k1").job().id();
        JobService afterConfigChange = service(5, Duration.ofSeconds(60));
        JobService.SubmitOutcome replay = afterConfigChange.submit(withPayload("{\"durationMs\":5}"), "k1");
        assertThat(replay.created()).isFalse();
        assertThat(replay.job().id()).isEqualTo(id);
        assertThat(replay.job().maxAttempts()).as("the stored job keeps its original value").isEqualTo(3);
    }

    @Test
    void explicitlyDifferentLimitsAreStillAConflict() throws Exception {
        JobService service = service(3, Duration.ofSeconds(30));
        service.submit(withPayload("{\"durationMs\":5}"), "k2");
        assertThatThrownBy(() -> service.submit(new SubmitJobRequest("sleep", null,
                mapper.readTree("{\"durationMs\":5}"), 4, null), "k2"))
                .isInstanceOf(IdempotencyConflictException.class);
        assertThatThrownBy(() -> service.submit(new SubmitJobRequest("sleep", null,
                mapper.readTree("{\"durationMs\":5}"), null, 1_000L), "k2"))
                .isInstanceOf(IdempotencyConflictException.class);
        assertThat(service.submit(new SubmitJobRequest("sleep", null, mapper.readTree("{\"durationMs\":5}"), 3,
                30_000L), "k2").created()).as("explicit values equal to the stored ones").isFalse();
    }
}
