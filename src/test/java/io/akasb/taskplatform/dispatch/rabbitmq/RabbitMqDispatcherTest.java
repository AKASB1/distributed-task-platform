package io.akasb.taskplatform.dispatch.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Parts of {@link RabbitMqDispatcher} that need no broker: settings validation, the message format, and the
 * behaviour when the broker cannot be reached (construction succeeds, dispatch and poll throw, depth is unknown).
 * The broker-backed behaviour is covered by {@code RabbitMqDispatcherIT}.
 */
class RabbitMqDispatcherTest {

    private static RabbitMqDispatcher.Settings settings(String host, int port, int prefetch) {
        return new RabbitMqDispatcher.Settings(host, port, "user", "s3cret", "/", "tp.", prefetch);
    }

    @Test
    void settingsAreValidated() {
        assertThatThrownBy(() -> settings(" ", 5672, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> settings("h", 0, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> settings("h", 65_536, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> settings("h", 5672, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> settings("h", 5672, 65_536)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RabbitMqDispatcher.Settings("h", 5672, null, "p", "/", "", 1))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new RabbitMqDispatcher.Settings("h", 5672, "u", null, "/", "", 1))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new RabbitMqDispatcher.Settings("h", 5672, "u", "p", "", "", 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RabbitMqDispatcher.Settings("h", 5672, "u", "p", "/", null, 1))
                .isInstanceOf(NullPointerException.class);
        assertThat(new RabbitMqDispatcher.Settings("h", 1, "", "", "/", "", 65_535).prefetch()).isEqualTo(65_535);
    }

    @Test
    void settingsNeverPrintThePassword() {
        String text = settings("broker.local", 5672, 16).toString();
        assertThat(text).doesNotContain("s3cret").contains("password=****", "host=broker.local", "prefetch=16");
    }

    @Test
    void onlyCanonicalUuidStringsAreJobIds() {
        UUID id = UUID.fromString("3f2b8c4e-1d2a-4b6c-9e8f-0a1b2c3d4e5f");
        assertThat(RabbitMqDispatcher.parseJobId(bytes(id.toString()))).isEqualTo(id);
        assertThat(RabbitMqDispatcher.parseJobId(bytes(id.toString().toUpperCase()))).isEqualTo(id);
        assertThat(RabbitMqDispatcher.parseJobId(null)).isNull();
        assertThat(RabbitMqDispatcher.parseJobId(new byte[0])).isNull();
        assertThat(RabbitMqDispatcher.parseJobId(bytes("not-a-uuid"))).isNull();
        assertThat(RabbitMqDispatcher.parseJobId(bytes("1-2-3-4-5"))).as("lenient short form rejected").isNull();
        assertThat(RabbitMqDispatcher.parseJobId(bytes(" " + id))).as("surrounding whitespace rejected").isNull();
        assertThat(RabbitMqDispatcher.parseJobId(bytes("zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz"))).isNull();
        assertThat(RabbitMqDispatcher.parseJobId(bytes("3f2b8c4e11d2a14b6c19e8f10a1b2c3d4e5f"))).isNull();
    }

    @Test
    @Timeout(60)
    void anUnreachableBrokerMakesDispatchAndPollFailButNotConstruction() throws Exception {
        int port = closedPort();
        RabbitMqDispatcher dispatcher = new RabbitMqDispatcher(settings("127.0.0.1", port, 4));
        try {
            assertThat(dispatcher.durable()).isTrue();
            assertThat(dispatcher.rabbitQueue("default")).isEqualTo("tp.default");
            // JobLifecycle logs this and leaves the job QUEUED for the reconciler.
            assertThatThrownBy(() -> dispatcher.dispatch("default", UUID.randomUUID()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("127.0.0.1:" + port);
            // WorkerPool logs this and polls again after its poll timeout.
            assertThatThrownBy(() -> dispatcher.poll("default", Duration.ofMillis(10)))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(dispatcher.depth("default")).isEqualTo(-1);
        } finally {
            dispatcher.close();
        }
        dispatcher.close(); // idempotent
        assertThatThrownBy(() -> dispatcher.dispatch("default", UUID.randomUUID()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
        assertThatThrownBy(() -> dispatcher.poll("default", Duration.ZERO))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
        assertThat(dispatcher.depth("default")).isEqualTo(-1);
    }

    @Test
    @Timeout(60)
    void pollOfAnInterruptedThreadThrowsBeforeTouchingTheBroker() throws Exception {
        RabbitMqDispatcher dispatcher = new RabbitMqDispatcher(settings("127.0.0.1", closedPort(), 4));
        try {
            Thread.currentThread().interrupt();
            assertThatThrownBy(() -> dispatcher.poll("default", Duration.ofSeconds(5)))
                    .isInstanceOf(InterruptedException.class);
        } finally {
            Thread.interrupted(); // never leak the flag into other tests
            dispatcher.close();
        }
    }

    @Test
    void nullArgumentsAreRejected() throws Exception {
        RabbitMqDispatcher dispatcher = new RabbitMqDispatcher(settings("127.0.0.1", closedPort(), 4));
        try {
            assertThatThrownBy(() -> dispatcher.dispatch("q", null)).isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> dispatcher.dispatch(null, UUID.randomUUID()))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> new RabbitMqDispatcher(null)).isInstanceOf(NullPointerException.class);
        } finally {
            dispatcher.close();
        }
    }

    /** A loopback port nobody listens on (bound once to pick it, then released). */
    private static int closedPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        }
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
