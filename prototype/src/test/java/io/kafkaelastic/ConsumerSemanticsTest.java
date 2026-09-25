package io.kafkaelastic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class ConsumerSemanticsTest {

    private static final Instant START =
            Instant.parse("2026-09-25T15:00:00Z");

    private ResizeController controller(Clock clock) {
        return new ResizeController(
                clock,
                Duration.ofMinutes(5),
                Duration.ofDays(30));
    }

    private void retireUpperFivePartitions(TopicState topic) {
        ResizeController startController =
                controller(Clock.fixed(START, ZoneOffset.UTC));

        startController.shrink(topic, 5);

        ResizeController afterGrace =
                controller(Clock.fixed(
                        START.plus(Duration.ofMinutes(6)),
                        ZoneOffset.UTC));

        afterGrace.completeTransition(topic);
    }

    @Test
    void existingConsumerCanReadRetiredPartition() {
        TopicState topic = new TopicState("orders", 10);

        ConsumerGroupState group =
                new ConsumerGroupState(
                        "pricing-service",
                        "orders",
                        true);

        SimulatedConsumer consumer =
                new SimulatedConsumer(topic, group);

        retireUpperFivePartitions(topic);
        consumer.refreshMetadata();

        assertEquals(
                PartitionLifecycleState.RETIRED,
                topic.partition(7).lifecycleState());

        assertTrue(consumer.canRead(7));
        assertEquals(10, consumer.readablePartitionIds().size());
    }

    @Test
    void newConsumerCreatedAfterResizeCanReadRetiredPartition() {
        TopicState topic = new TopicState("orders", 10);

        retireUpperFivePartitions(topic);

        ConsumerGroupState newGroup =
                new ConsumerGroupState(
                        "replay-tool",
                        "orders",
                        true);

        SimulatedConsumer newConsumer =
                new SimulatedConsumer(topic, newGroup);

        assertTrue(newConsumer.canRead(7));
        assertEquals(10, newConsumer.readablePartitionIds().size());
    }

    @Test
    void groupPresentAtRetirementCanBeDrainTracked() {
        TopicState topic = new TopicState("orders", 10);

        ConsumerGroupState existingGroup =
                new ConsumerGroupState(
                        "pricing-service",
                        "orders",
                        true);

        long epoch = controller(
                Clock.fixed(START, ZoneOffset.UTC))
                .shrink(topic, 5);

        DrainTracker tracker = new DrainTracker(
                "orders",
                epoch,
                List.of(existingGroup));

        assertTrue(tracker.isTracked("pricing-service"));
    }

    @Test
    void newGroupAfterRetirementIsNotAutomaticallyDrainTracked() {
        TopicState topic = new TopicState("orders", 10);

        ConsumerGroupState existingGroup =
                new ConsumerGroupState(
                        "pricing-service",
                        "orders",
                        true);

        long epoch = controller(
                Clock.fixed(START, ZoneOffset.UTC))
                .shrink(topic, 5);

        DrainTracker tracker = new DrainTracker(
                "orders",
                epoch,
                List.of(existingGroup));

        ConsumerGroupState laterGroup =
                new ConsumerGroupState(
                        "replay-tool",
                        "orders",
                        true);

        assertFalse(tracker.isTracked("replay-tool"));
    }

    @Test
    void committedOffsetDeterminesDrainProgress() {
        ConsumerGroupState group =
                new ConsumerGroupState(
                        "audit-service",
                        "orders",
                        true);

        group.commit(7, 820_000L);

        DrainTracker tracker = new DrainTracker(
                "orders",
                42L,
                List.of(group));

        assertEquals(
                DrainStatus.LAGGING,
                tracker.status(
                        "audit-service",
                        7,
                        850_001L));

        group.commit(7, 850_001L);

        assertEquals(
                DrainStatus.DRAINED,
                tracker.status(
                        "audit-service",
                        7,
                        850_001L));
    }

    @Test
    void groupWithCommittedOffsetsIsRelevantEvenIfCurrentlyInactive() {
        ConsumerGroupState inactiveGroup =
                new ConsumerGroupState(
                        "audit-service",
                        "orders",
                        false);

        inactiveGroup.commit(7, 123L);

        DrainTracker tracker = new DrainTracker(
                "orders",
                42L,
                List.of(inactiveGroup));

        assertTrue(tracker.isTracked("audit-service"));
    }

    @Test
    void unrelatedInactiveGroupIsNotDrainTracked() {
        ConsumerGroupState inactiveGroup =
                new ConsumerGroupState(
                        "unused-group",
                        "orders",
                        false);

        DrainTracker tracker = new DrainTracker(
                "orders",
                42L,
                List.of(inactiveGroup));

        assertFalse(tracker.isTracked("unused-group"));
    }

    @Test
    void administratorCanAddAndRemoveDrainTrackedGroup() {
        ConsumerGroupState group =
                new ConsumerGroupState(
                        "replay-tool",
                        "orders",
                        true);

        DrainTracker tracker =
                new DrainTracker(
                        "orders",
                        42L,
                        List.of());

        assertFalse(tracker.isTracked("replay-tool"));

        tracker.addGroup(group);
        assertTrue(tracker.isTracked("replay-tool"));

        tracker.removeGroup("replay-tool");
        assertFalse(tracker.isTracked("replay-tool"));
    }
}
