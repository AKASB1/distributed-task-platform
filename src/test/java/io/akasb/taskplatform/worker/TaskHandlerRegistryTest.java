package io.akasb.taskplatform.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.akasb.taskplatform.worker.handlers.FlakyTaskHandler;
import io.akasb.taskplatform.worker.handlers.SleepTaskHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Job type to handler mapping: lookup, duplicate rejection, and the exposed type set. */
class TaskHandlerRegistryTest {

    /** Minimal handler with a configurable type. */
    private record Named(String type) implements TaskHandler {
        @Override
        public Object handle(TaskContext context) {
            return type;
        }
    }

    @Test
    void findsHandlersByType() {
        SleepTaskHandler sleep = new SleepTaskHandler();
        FlakyTaskHandler flaky = new FlakyTaskHandler();
        TaskHandlerRegistry registry = new TaskHandlerRegistry(List.of(sleep, flaky));

        assertThat(registry.find("sleep")).containsSame(sleep);
        assertThat(registry.find("flaky")).containsSame(flaky);
        assertThat(registry.find("missing")).isEmpty();
        assertThat(registry.find("Sleep")).as("types are case-sensitive").isEmpty();
        assertThat(registry.find(null)).isEmpty();
    }

    @Test
    void typesListsEveryRegisteredType() {
        TaskHandlerRegistry registry = new TaskHandlerRegistry(
                List.of(new SleepTaskHandler(), new FlakyTaskHandler(), new Named("custom")));
        assertThat(registry.types()).containsExactlyInAnyOrder("sleep", "flaky", "custom");
    }

    @Test
    void typesIsAnImmutableSnapshot() {
        TaskHandlerRegistry registry = new TaskHandlerRegistry(List.of(new Named("a")));
        Set<String> types = registry.types();
        assertThatThrownBy(() -> types.add("b")).isInstanceOf(UnsupportedOperationException.class);
        assertThat(registry.types()).containsExactly("a");
        assertThat(registry.find("b")).isEmpty();
    }

    @Test
    void laterChangesToTheSourceCollectionDoNotLeakIn() {
        List<TaskHandler> source = new ArrayList<>(List.of(new Named("a")));
        TaskHandlerRegistry registry = new TaskHandlerRegistry(source);
        source.add(new Named("b"));
        assertThat(registry.types()).containsExactly("a");
        assertThat(registry.find("b")).isEmpty();
    }

    @Test
    void emptyRegistryHasNoTypes() {
        TaskHandlerRegistry registry = new TaskHandlerRegistry(List.of());
        assertThat(registry.types()).isEmpty();
        assertThat(registry.find("sleep")).isEmpty();
    }

    @Test
    void rejectsTwoHandlersForTheSameType() {
        Named first = new Named("dup");
        Named second = new Named("dup");
        assertThatThrownBy(() -> new TaskHandlerRegistry(List.of(new Named("ok"), first, second)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("two handlers for type dup");
    }

    @Test
    void rejectsTheSameHandlerInstanceTwice() {
        SleepTaskHandler sleep = new SleepTaskHandler();
        assertThatThrownBy(() -> new TaskHandlerRegistry(List.of(sleep, sleep)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("two handlers for type sleep");
    }

    @Test
    void rejectsANullCollection() {
        assertThatThrownBy(() -> new TaskHandlerRegistry(null)).isInstanceOf(NullPointerException.class);
    }
}
