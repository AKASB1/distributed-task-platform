package io.akasb.taskplatform.domain;

import static io.akasb.taskplatform.domain.JobState.CANCELLED;
import static io.akasb.taskplatform.domain.JobState.DEAD_LETTER;
import static io.akasb.taskplatform.domain.JobState.FAILED;
import static io.akasb.taskplatform.domain.JobState.PENDING;
import static io.akasb.taskplatform.domain.JobState.QUEUED;
import static io.akasb.taskplatform.domain.JobState.RETRY_WAIT;
import static io.akasb.taskplatform.domain.JobState.RUNNING;
import static io.akasb.taskplatform.domain.JobState.SUCCEEDED;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Every one of the 8 × 8 (from, to) pairs is checked against an independently written table. */
class JobStateTest {

    private static final Map<JobState, Set<JobState>> LEGAL = Map.of(
            PENDING, EnumSet.of(QUEUED, CANCELLED),
            QUEUED, EnumSet.of(RUNNING, CANCELLED),
            RUNNING, EnumSet.of(SUCCEEDED, RETRY_WAIT, FAILED, DEAD_LETTER, CANCELLED),
            RETRY_WAIT, EnumSet.of(QUEUED, CANCELLED),
            SUCCEEDED, EnumSet.noneOf(JobState.class),
            FAILED, EnumSet.noneOf(JobState.class),
            DEAD_LETTER, EnumSet.noneOf(JobState.class),
            CANCELLED, EnumSet.noneOf(JobState.class));

    static Stream<Arguments> allPairs() {
        List<Arguments> pairs = new ArrayList<>();
        for (JobState from : JobState.values()) {
            for (JobState to : JobState.values()) {
                pairs.add(Arguments.of(from, to, LEGAL.get(from).contains(to)));
            }
        }
        return pairs.stream();
    }

    @ParameterizedTest(name = "{0} -> {1} legal={2}")
    @MethodSource("allPairs")
    void transitionMatrix(JobState from, JobState to, boolean legal) {
        assertThat(from.canTransitionTo(to)).isEqualTo(legal);
    }

    @Test
    void matrixCoversEveryState() {
        assertThat(LEGAL.keySet()).containsExactlyInAnyOrder(JobState.values());
        assertThat(allPairs()).hasSize(64);
        long legalCount = allPairs().filter(a -> (boolean) a.get()[2]).count();
        assertThat(legalCount).isEqualTo(11);
    }

    @Test
    void terminalStatesAreExactlyThoseWithoutExits() {
        assertThat(EnumSet.allOf(JobState.class).stream().filter(JobState::isTerminal))
                .containsExactlyInAnyOrder(SUCCEEDED, FAILED, DEAD_LETTER, CANCELLED);
    }

    @Test
    void noSelfTransitions() {
        for (JobState s : JobState.values()) assertThat(s.canTransitionTo(s)).as(s.name()).isFalse();
    }

    @Test
    void allowedNextMatchesTable() {
        for (JobState s : JobState.values()) assertThat(s.allowedNext()).isEqualTo(LEGAL.get(s));
    }
}
