package io.akasb.taskplatform.worker;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import io.akasb.taskplatform.dispatch.Checkpoints;
import io.akasb.taskplatform.dispatch.InMemoryDispatcher;
import io.akasb.taskplatform.dispatch.JobLifecycle;
import io.akasb.taskplatform.dispatch.SubmitCommand;
import io.akasb.taskplatform.domain.AckState;
import io.akasb.taskplatform.domain.Delivery;
import io.akasb.taskplatform.domain.Job;
import io.akasb.taskplatform.domain.JobState;
import io.akasb.taskplatform.domain.RetryPolicy;
import io.akasb.taskplatform.observability.LifecycleListener;
import io.akasb.taskplatform.persistence.JdbcJobRepository;
import io.akasb.taskplatform.support.Await;
import io.akasb.taskplatform.support.EmbeddedPostgresSupport;
import io.akasb.taskplatform.support.MutableClock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Handler output that PostgreSQL cannot store as is, end to end on real PostgreSQL: a U+0000 in a result or an error
 * message (jsonb and text reject it) and a result that cannot be serialised. Each must end in the right terminal state
 * after one attempt instead of being lost and re-run through lease expiry.
 */
class HandlerOutputIT {
    private static final String QUEUE = "output";
    private final ObjectMapper mapper = new ObjectMapper();
    private HikariDataSource dataSource;
    private JdbcJobRepository repository;
    private JobLifecycle lifecycle;
    private WorkerPool pool;

    @BeforeEach
    void setUp() {
        dataSource = EmbeddedPostgresSupport.newMigratedDataSource(8);
        repository = new JdbcJobRepository(dataSource);
        InMemoryDispatcher dispatcher = new InMemoryDispatcher();
        MutableClock clock = MutableClock.startingAt("2026-01-01T00:00:00Z");
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(1), 2.0, Duration.ofMinutes(1), 0.5, new Random(3));
        lifecycle = new JobLifecycle(repository, dispatcher, retry, clock, Duration.ofSeconds(30),
                LifecycleListener.none(), Checkpoints.none());
        List<TaskHandler> handlers = List.of(
                handler("nul-result", c -> Map.of("text", "a\u0000b", "k\u0000ey", List.of("x\u0000"))),
                handler("nul-error", c -> {
                    throw new NonRetryableTaskException("bad record a\u0000b");
                }),
                handler("unserialisable", c -> new Object() {
                    @SuppressWarnings("unused")
                    public Object getSelf() {
                        throw new IllegalStateException("boom");
                    }
                }));
        pool = new WorkerPool(new WorkerPool.Settings("output", QUEUE, 2, Duration.ofMillis(20),
                Duration.ofMillis(50), Duration.ofSeconds(1)), new LocalWorkerProtocol(dispatcher, lifecycle),
                new TaskHandlerRegistry(handlers), clock, mapper);
        pool.start();
    }

    @AfterEach
    void tearDown() {
        try {
            pool.close();
        } finally {
            dataSource.close();
        }
    }

    @Test
    void nulCharactersInAResultAreReplacedAndTheJobSucceedsOnce() throws Exception {
        Job done = await(submit("nul-result"), JobState.SUCCEEDED);
        assertThat(mapper.readTree(done.result())).isEqualTo(mapper.readTree(
                "{\"text\":\"a�b\",\"k�ey\":[\"x�\"]}"));
        assertThat(repository.deliveries(done.id())).singleElement()
                .extracting(Delivery::ackState).isEqualTo(AckState.ACKED);
    }

    @Test
    void nulCharacterInAPermanentErrorIsStoredAndTheJobFailsOnce() {
        Job done = await(submit("nul-error"), JobState.FAILED);
        assertThat(done.attempts()).isEqualTo(1);
        assertThat(done.lastError()).isEqualTo("NON_RETRYABLE: bad record a�b");
        assertThat(repository.deliveries(done.id())).singleElement()
                .extracting(Delivery::ackState).isEqualTo(AckState.NACKED);
    }

    @Test
    void aResultThatCannotBeSerialisedFailsTheJobInsteadOfStoringNull() {
        Job done = await(submit("unserialisable"), JobState.FAILED);
        assertThat(done.attempts()).isEqualTo(1);
        assertThat(done.result()).isNull();
        assertThat(done.lastError()).startsWith("NON_RETRYABLE: handler result is not JSON-serialisable");
    }

    private UUID submit(String type) {
        return lifecycle.submit(new SubmitCommand(QUEUE, type, "{}", 3, Duration.ofSeconds(30), null)).job().id();
    }

    private Job await(UUID id, JobState state) {
        return Await.value("job " + id + " reaches " + state, Duration.ofSeconds(20),
                () -> repository.find(id).orElseThrow(), j -> j.state() == state);
    }

    private static TaskHandler handler(String type, Body body) {
        return new TaskHandler() {
            @Override
            public String type() {
                return type;
            }

            @Override
            public Object handle(TaskContext context) throws Exception {
                return body.run(context);
            }
        };
    }

    @FunctionalInterface
    private interface Body {
        Object run(TaskContext context) throws Exception;
    }
}
