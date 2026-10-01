package io.akasb.taskplatform.cache;

/** Redis is not reachable right now (no connection, or waiting before the next connection attempt). */
public final class RedisUnavailableException extends RuntimeException {
    public RedisUnavailableException(String message) {
        super(message);
    }

    public RedisUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
