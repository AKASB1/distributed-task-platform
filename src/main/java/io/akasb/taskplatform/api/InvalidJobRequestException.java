package io.akasb.taskplatform.api;

/** The request is well-formed JSON but not acceptable (unknown type, unserved queue, limits). Maps to 400. */
public final class InvalidJobRequestException extends RuntimeException {
    public InvalidJobRequestException(String message) {
        super(message);
    }
}
