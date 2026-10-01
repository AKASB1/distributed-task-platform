package io.akasb.taskplatform.worker;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Maps job types to handlers. */
public final class TaskHandlerRegistry {
    private final Map<String, TaskHandler> handlers = new LinkedHashMap<>();

    public TaskHandlerRegistry(Collection<? extends TaskHandler> handlers) {
        for (TaskHandler h : handlers) {
            if (this.handlers.putIfAbsent(h.type(), h) != null) {
                throw new IllegalArgumentException("two handlers for type " + h.type());
            }
        }
    }

    public Optional<TaskHandler> find(String type) {
        return Optional.ofNullable(handlers.get(type));
    }

    public Set<String> types() {
        return Set.copyOf(handlers.keySet());
    }
}
