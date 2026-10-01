package io.akasb.taskplatform;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.akasb.taskplatform.dispatch.JobDispatcher;
import io.akasb.taskplatform.dispatch.rabbitmq.RabbitMqDispatcher;
import io.akasb.taskplatform.support.Await;
import io.akasb.taskplatform.support.PostgresTestProperties;
import io.akasb.taskplatform.support.RabbitMqTestContainer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The whole service with {@code taskplatform.dispatcher.type=rabbitmq}: embedded PostgreSQL plus a RabbitMQ broker
 * (Testcontainers, skipped without Docker). Jobs submitted over HTTP travel through RabbitMQ to the worker pool and
 * finish; afterwards the job queue's RabbitMQ queue is empty.
 *
 * <p>{@link DirtiesContext} closes the application (and its broker connections) after this class, before the broker
 * container stops, so the cached context does not keep reconnecting to a broker that is gone.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "taskplatform.dispatcher.type=rabbitmq",
        "taskplatform.rabbitmq.queue-prefix=e2e.",
        "taskplatform.heartbeat-interval=200ms",
        "taskplatform.reconciler.interval=100ms",
        "taskplatform.retry.initial-delay=50ms",
        "taskplatform.retry.max-delay=200ms",
        "taskplatform.retry.seed=11"})
@DirtiesContext
class RabbitMqEndToEndIT {
    @Container
    static final RabbitMQContainer BROKER = RabbitMqTestContainer.create();

    private static final Duration WAIT = Duration.ofSeconds(30);
    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();

    @LocalServerPort
    int port;

    @Autowired
    JobDispatcher dispatcher;

    @DynamicPropertySource
    static void services(DynamicPropertyRegistry registry) {
        PostgresTestProperties.register(registry);
        registry.add("taskplatform.rabbitmq.host", BROKER::getHost);
        registry.add("taskplatform.rabbitmq.port", BROKER::getAmqpPort);
        registry.add("taskplatform.rabbitmq.username", BROKER::getAdminUsername);
        registry.add("taskplatform.rabbitmq.password", BROKER::getAdminPassword);
        registry.add("taskplatform.rabbitmq.virtual-host", () -> "/");
    }

    @Test
    void theDispatcherIsTheRabbitMqAdapter() {
        assertThat(dispatcher).isInstanceOf(RabbitMqDispatcher.class);
        assertThat(dispatcher.durable()).isTrue();
        assertThat(((RabbitMqDispatcher) dispatcher).rabbitQueue("default")).isEqualTo("e2e.default");
    }

    @Test
    void aSleepJobSucceedsThroughRabbitMq() throws Exception {
        String id = submit("{\"type\":\"sleep\",\"payload\":{\"durationMs\":20}}");

        JsonNode done = Await.value("job succeeded", WAIT, () -> get("/v1/jobs/" + id),
                j -> "SUCCEEDED".equals(j.path("state").asText()));
        assertThat(done.path("deliveries")).hasSize(1);
        assertThat(done.path("deliveries").get(0).path("ackState").asText()).isEqualTo("ACKED");
        assertQueueDrained();
    }

    @Test
    void aFlakyJobIsRetriedThroughRabbitMqAndSucceeds() throws Exception {
        String id = submit("{\"type\":\"flaky\",\"maxAttempts\":3,\"payload\":{\"durationMs\":10,\"failAttempts\":1}}");

        JsonNode done = Await.value("job succeeded", WAIT, () -> get("/v1/jobs/" + id),
                j -> "SUCCEEDED".equals(j.path("state").asText()));
        assertThat(done.path("attempts").asInt()).isEqualTo(2);
        assertThat(done.path("deliveries")).hasSize(2);
        assertThat(done.path("deliveries").get(0).path("ackState").asText()).isEqualTo("NACKED");
        assertThat(done.path("deliveries").get(1).path("ackState").asText()).isEqualTo("ACKED");
        assertThat(done.path("result").path("succeededOnAttempt").asInt()).isEqualTo(2);
        assertQueueDrained();
    }

    /** Nothing ready in the broker, nothing buffered by the service's consumer. */
    private void assertQueueDrained() throws Exception {
        Await.value("dispatcher depth of queue default", WAIT, () -> dispatcher.depth("default"), n -> n == 0);
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(BROKER.getHost());
        factory.setPort(BROKER.getAmqpPort());
        factory.setUsername(BROKER.getAdminUsername());
        factory.setPassword(BROKER.getAdminPassword());
        factory.setAutomaticRecoveryEnabled(false);
        try (Connection connection = factory.newConnection("e2e-test-admin");
             Channel channel = connection.createChannel()) {
            assertThat(channel.queueDeclarePassive("e2e.default").getMessageCount()).isZero();
            assertThat(channel.consumerCount("e2e.default")).as("the service consumes the queue").isEqualTo(1);
        }
    }

    private String submit(String body) throws Exception {
        HttpResponse<String> created = http.send(HttpRequest.newBuilder(uri("/v1/jobs"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(created.statusCode()).as(created.body()).isEqualTo(201);
        return mapper.readTree(created.body()).get("id").asText();
    }

    private JsonNode get(String path) {
        try {
            return mapper.readTree(http.send(HttpRequest.newBuilder(uri(path)).build(),
                    HttpResponse.BodyHandlers.ofString()).body());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private URI uri(String path) {
        return URI.create("http://127.0.0.1:" + port + path);
    }
}
