package io.kafkaelastic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class ResizeControllerTest {

    private static final Instant NOW =
            Instant.parse("2026-09-25T15:00:00Z");

    private ResizeController controller(Clock clock) {
        return new ResizeController(
                clock,
                Duration.ofMinutes(5),
                Duration.ofDays(30));
    }

    @Test
    void shrinkTenToFiveThenRetire() {
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        ResizeController controller = controller(clock);
        TopicState topic = new TopicState("orders", 10);

        long epoch = controller.shrink(topic, 5);

        assertEquals(1L, epoch);
        assertEquals(5L, topic.activePartitionCount());
        assertEquals(5L, topic.transitioningPartitionCount());
        assertTrue(topic.resizeInProgress());

        assertEquals(
                PartitionLifecycleState.TRANSITIONING,
                topic.partition(9).lifecycleState());
    }

    @Test
    void transitionCannotCompleteBeforeGraceExpires() {
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        ResizeController controller = controller(clock);
        TopicState topic = new TopicState("orders", 10);

        controller.shrink(topic, 5);

        assertThrows(
                ResizeException.class,
                () -> controller.completeTransition(topic));
    }

    @Test
    void completesRetirementAfterGrace() {
        Clock start = Clock.fixed(NOW, ZoneOffset.UTC);
        TopicState topic = new TopicState("orders", 10);

        ResizeController firstController = controller(start);
        firstController.shrink(topic, 5);

        Clock afterGrace =
                Clock.fixed(NOW.plus(Duration.ofMinutes(6)), ZoneOffset.UTC);

        ResizeController resumedController = controller(afterGrace);
        resumedController.completeTransition(topic);

        assertEquals(5L, topic.activePartitionCount());
        assertEquals(0L, topic.transitioningPartitionCount());
        assertEquals(5L, topic.retiredPartitionCount());
        assertFalse(topic.resizeInProgress());

        assertEquals(
                PartitionLifecycleState.RETIRED,
                topic.partition(7).lifecycleState());
    }

    @Test
    void reactivatesRetiredPartitionsAndPreservesPartitionIdentity() {
        TopicState topic = new TopicState("orders", 10);

        ResizeController shrinkController =
                controller(Clock.fixed(NOW, ZoneOffset.UTC));

        shrinkController.shrink(topic, 5);

        ResizeController afterGrace =
                controller(Clock.fixed(
                        NOW.plus(Duration.ofMinutes(6)),
                        ZoneOffset.UTC));

        afterGrace.completeTransition(topic);

        long shrinkEpoch = topic.currentResizeEpoch();
        PartitionState p7Before = topic.partition(7);

        long expansionEpoch = afterGrace.expand(topic, 10);

        PartitionState p7After = topic.partition(7);

        assertEquals(1L, shrinkEpoch);
        assertEquals(2L, expansionEpoch);
        assertTrue(p7Before == p7After);
        assertEquals(
                PartitionLifecycleState.ACTIVE,
                p7After.lifecycleState());
        assertEquals(10L, topic.activePartitionCount());
        assertEquals(0L, topic.retiredPartitionCount());
    }

    @Test
    void staleEpochCannotMutateReactivatedPartition() {
        TopicState topic = new TopicState("orders", 10);

        ResizeController shrinkController =
                controller(Clock.fixed(NOW, ZoneOffset.UTC));

        long shrinkEpoch = shrinkController.shrink(topic, 5);

        ResizeController afterGrace =
                controller(Clock.fixed(
                        NOW.plus(Duration.ofMinutes(6)),
                        ZoneOffset.UTC));

        afterGrace.completeTransition(topic);
        long expansionEpoch = afterGrace.expand(topic, 10);

        PartitionState p7 = topic.partition(7);

        assertEquals(1L, shrinkEpoch);
        assertEquals(2L, expansionEpoch);
        assertEquals(2L, p7.lifecycleEpoch());

        assertThrows(
                StaleResizeEpochException.class,
                () -> afterGrace.assertCurrentEpoch(p7, shrinkEpoch));
    }

    @Test
    void rejectsSecondResizeWhileTransitionIsInProgress() {
        TopicState topic = new TopicState("orders", 10);
        ResizeController controller =
                controller(Clock.fixed(NOW, ZoneOffset.UTC));

        controller.shrink(topic, 5);

        assertThrows(
                ResizeInProgressException.class,
                () -> controller.shrink(topic, 3));
    }
}
