package io.akasb.taskplatform.domain;

import static io.akasb.taskplatform.domain.AckState.ACKED;
import static io.akasb.taskplatform.domain.AckState.CANCELLED;
import static io.akasb.taskplatform.domain.AckState.EXPIRED;
import static io.akasb.taskplatform.domain.AckState.LEASED;
import static io.akasb.taskplatform.domain.AckState.NACKED;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Every one of the 5 × 5 delivery-state pairs: only LEASED is open and it may only move to a final state. */
class AckStateTest {
    private static final Set<AckState> FINAL = EnumSet.of(ACKED, NACKED, EXPIRED, CANCELLED);

    static Stream<Arguments> allPairs() {
        List<Arguments> pairs = new ArrayList<>();
        for (AckState from : AckState.values()) {
            for (AckState to : AckState.values()) {
                pairs.add(Arguments.of(from, to, from == LEASED && FINAL.contains(to)));
            }
        }
        return pairs.stream();
    }

    @ParameterizedTest(name = "{0} -> {1} legal={2}")
    @MethodSource("allPairs")
    void transitionMatrix(AckState from, AckState to, boolean legal) {
        assertThat(from.canTransitionTo(to)).isEqualTo(legal);
    }

    @Test
    void matrixHasExactlyFourLegalPairs() {
        assertThat(allPairs()).hasSize(25);
        assertThat(allPairs().filter(a -> (boolean) a.get()[2]).count()).isEqualTo(4);
    }

    @Test
    void onlyLeasedIsOpen() {
        assertThat(EnumSet.allOf(AckState.class).stream().filter(AckState::isOpen)).containsExactly(LEASED);
        assertThat(EnumSet.complementOf(EnumSet.of(LEASED))).isEqualTo(FINAL);
    }

    @Test
    void finalStatesHaveNoExits() {
        for (AckState s : FINAL) {
            for (AckState t : AckState.values()) assertThat(s.canTransitionTo(t)).as(s + "->" + t).isFalse();
        }
    }
}
