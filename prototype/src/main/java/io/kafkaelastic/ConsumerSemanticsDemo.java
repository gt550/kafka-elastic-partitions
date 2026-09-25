package io.kafkaelastic;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

public final class ConsumerSemanticsDemo {

    public static void main(String[] args) {
        Instant start = Instant.parse("2026-09-25T15:00:00Z");

        TopicState topic = new TopicState("orders", 10);

        ConsumerGroupState existingGroup =
                new ConsumerGroupState(
                        "pricing-service",
                        "orders",
                        true);

        existingGroup.commit(7, 820_000L);

        SimulatedConsumer existingConsumer =
                new SimulatedConsumer(topic, existingGroup);

        ResizeController startController = new ResizeController(
                Clock.fixed(start, ZoneOffset.UTC),
                Duration.ofMinutes(5),
                Duration.ofDays(30));

        long resizeEpoch = startController.shrink(topic, 5);

        DrainTracker drainTracker = new DrainTracker(
                "orders",
                resizeEpoch,
                List.of(existingGroup));

        ResizeController afterGraceController = new ResizeController(
                Clock.fixed(
                        start.plus(Duration.ofMinutes(6)),
                        ZoneOffset.UTC),
                Duration.ofMinutes(5),
                Duration.ofDays(30));

        afterGraceController.completeTransition(topic);

        existingConsumer.refreshMetadata();

        ConsumerGroupState newGroup =
                new ConsumerGroupState(
                        "replay-tool",
                        "orders",
                        true);

        SimulatedConsumer newConsumer =
                new SimulatedConsumer(topic, newGroup);

        System.out.println(
                "P7 lifecycle: " + topic.partition(7).lifecycleState());

        System.out.println(
                "Existing consumer can read P7: "
                        + existingConsumer.canRead(7));

        System.out.println(
                "New consumer can read P7: "
                        + newConsumer.canRead(7));

        System.out.println(
                "pricing-service drain tracked: "
                        + drainTracker.isTracked("pricing-service"));

        System.out.println(
                "replay-tool drain tracked: "
                        + drainTracker.isTracked("replay-tool"));

        System.out.println(
                "pricing-service status at P7 LEO 850001: "
                        + drainTracker.status(
                                "pricing-service",
                                7,
                                850_001L));

        existingConsumer.commit(7, 850_001L);

        System.out.println(
                "pricing-service status after commit: "
                        + drainTracker.status(
                                "pricing-service",
                                7,
                                850_001L));
    }
}
