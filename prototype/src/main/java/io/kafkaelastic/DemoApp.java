package io.kafkaelastic;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

public final class DemoApp {

    public static void main(String[] args) {
        Instant start = Instant.parse("2026-09-25T15:00:00Z");

        TopicState topic = new TopicState("orders", 10);

        ResizeController controller = new ResizeController(
                Clock.fixed(start, ZoneOffset.UTC),
                Duration.ofMinutes(5),
                Duration.ofDays(30));

        printState("Initial", topic);

        long shrinkEpoch = controller.shrink(topic, 5);
        printState("After shrink request, epoch " + shrinkEpoch, topic);

        ResizeController afterGrace = new ResizeController(
                Clock.fixed(start.plus(Duration.ofMinutes(6)), ZoneOffset.UTC),
                Duration.ofMinutes(5),
                Duration.ofDays(30));

        afterGrace.completeTransition(topic);
        printState("After transition completes", topic);

        long expandEpoch = afterGrace.expand(topic, 10);
        printState("After expansion, epoch " + expandEpoch, topic);
    }

    private static void printState(String label, TopicState topic) {
        System.out.println();
        System.out.println("=== " + label + " ===");

        for (PartitionState partition : topic.partitions()) {
            System.out.printf(
                    "P%-2d %-14s epoch=%d%n",
                    partition.partitionId(),
                    partition.lifecycleState(),
                    partition.lifecycleEpoch());
        }

        System.out.printf(
                "active=%d transitioning=%d retired=%d resizeInProgress=%s%n",
                topic.activePartitionCount(),
                topic.transitioningPartitionCount(),
                topic.retiredPartitionCount(),
                topic.resizeInProgress());
    }
}
