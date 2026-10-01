package io.akasb.taskplatform.support;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import io.akasb.taskplatform.dispatch.rabbitmq.RabbitMqDispatcher;
import java.util.Arrays;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * RabbitMQ broker for integration tests (Testcontainers): labelled {@code cvproject=distributed-task-platform} and
 * published on random loopback ports only. Test classes that use it are annotated
 * {@code @Testcontainers(disabledWithoutDocker = true)}, so they skip cleanly without Docker.
 */
public final class RabbitMqTestContainer {
    public static final String IMAGE = "rabbitmq:4-management-alpine";
    private static final int[] PORTS = {5672, 5671, 15672, 15671};

    private RabbitMqTestContainer() {}

    public static RabbitMQContainer create() {
        return new RabbitMQContainer(DockerImageName.parse(IMAGE))
                .withLabel("cvproject", "distributed-task-platform")
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                        .withPublishAllPorts(false)
                        .withPortBindings(Arrays.stream(PORTS)
                                .mapToObj(p -> new PortBinding(Ports.Binding.bindIp("127.0.0.1"), ExposedPort.tcp(p)))
                                .toList()));
    }

    /** Dispatcher settings for {@code broker} with the given queue prefix and prefetch. */
    public static RabbitMqDispatcher.Settings settings(RabbitMQContainer broker, String queuePrefix, int prefetch) {
        return new RabbitMqDispatcher.Settings(broker.getHost(), broker.getAmqpPort(), broker.getAdminUsername(),
                broker.getAdminPassword(), "/", queuePrefix, prefetch);
    }
}
