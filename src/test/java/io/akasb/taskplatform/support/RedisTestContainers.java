package io.akasb.taskplatform.support;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Redis for integration tests (Testcontainers; the tests using it carry
 * {@code @Testcontainers(disabledWithoutDocker = true)} and are skipped without Docker). Every container is labelled
 * {@code cvproject=distributed-task-platform}.
 */
public final class RedisTestContainers {
    public static final DockerImageName IMAGE = DockerImageName.parse("redis:7-alpine");
    public static final int PORT = 6379;

    private RedisTestContainers() {}

    @SuppressWarnings("resource") // the caller (or the Testcontainers extension) stops it
    public static GenericContainer<?> redis() {
        return new GenericContainer<>(IMAGE)
                .withExposedPorts(PORT)
                .withLabel("cvproject", "distributed-task-platform")
                .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));
    }

    public static String uri(GenericContainer<?> redis) {
        return "redis://" + redis.getHost() + ":" + redis.getMappedPort(PORT);
    }

    /** A loopback URI on which nothing listens (a port that was free a moment ago). */
    public static String unreachableUri() {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            return "redis://127.0.0.1:" + socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
