package io.akasb.taskplatform.dispatch;

import java.util.UUID;

public final class JobNotFoundException extends RuntimeException {
    public JobNotFoundException(UUID id) {
        super("job " + id + " not found");
    }
}
