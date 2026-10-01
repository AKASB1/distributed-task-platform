package io.akasb.taskplatform.cache;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import java.time.Duration;
import java.util.Objects;

/** Builds Lettuce clients configured for a cache that must never slow the service down for long. */
public final class RedisClients {
    private RedisClients() {}

    /**
     * A client for {@code uri} (for example {@code redis://127.0.0.1:6379}) whose connect and command timeouts are
     * {@code timeout}. Creating the client does not connect. Lettuce's own background reconnect is off and commands on
     * a broken connection are rejected at once: {@link RedisConnectionHolder} owns reconnection.
     */
    public static RedisClient create(String uri, Duration timeout) {
        Objects.requireNonNull(uri, "uri");
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative() || timeout.isZero()) throw new IllegalArgumentException("timeout must be positive");
        RedisURI redisUri = RedisURI.create(uri);
        redisUri.setTimeout(timeout);
        RedisClient client = RedisClient.create(redisUri);
        client.setOptions(ClientOptions.builder()
                .autoReconnect(false)
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .socketOptions(SocketOptions.builder().connectTimeout(timeout).build())
                .timeoutOptions(TimeoutOptions.enabled(timeout))
                .build());
        return client;
    }
}
