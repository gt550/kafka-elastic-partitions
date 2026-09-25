package io.kafkaelastic;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

public final class FailureRecoveryDemo {

    public static void main(String[] args) {
        Instant start = Instant.parse("2026-09-25T15:00:00Z");
        MetadataQuorum quorum = new MetadataQuorum(true);

        TopicState topic = new TopicState("orders", 10);

        ResizeController controller1 = new ResizeController(
                Clock.fixed(start, ZoneOffset.UTC),
                Duration.ofMinutes(5),
                Duration.ofDays(30),
                null,
                quorum);

        long epoch42 = controller1.shrink(topic, 5);

        System.out.println(
                "Controller 1 started epoch " + epoch42
                        + "; P7=" + topic.partition(7).lifecycleState());

        // Simulated controller failover: a new controller object takes over
        // the same durable topic metadata.
        ResizeController controller2 = new ResizeController(
                Clock.fixed(
                        start.plus(Duration.ofMinutes(6)),
                        ZoneOffset.UTC),
                Duration.ofMinutes(5),
                Duration.ofDays(30),
                null,
                quorum);

        quorum.setAvailable(false);

        try {
            controller2.completeTransition(topic);
        } catch (ControllerQuorumUnavailableException e) {
            System.out.println(
                    "Quorum unavailable; P7 remains "
                            + topic.partition(7).lifecycleState());
        }

        quorum.setAvailable(true);
        controller2.completeTransition(topic);

        System.out.println(
                "After quorum recovery; P7="
                        + topic.partition(7).lifecycleState());

        long epoch43 = controller2.expand(topic, 10);

        System.out.println(
                "Reactivated P7 under epoch " + epoch43);

        ResizeController day31Controller = new ResizeController(
                Clock.fixed(
                        start.plus(Duration.ofDays(31)),
                        ZoneOffset.UTC),
                Duration.ofMinutes(5),
                Duration.ofDays(30),
                null,
                quorum);

        System.out.println(
                "Delayed epoch-" + epoch42 + " delete of P7: "
                        + day31Controller.deleteRetiredPartition(
                                topic, 7, epoch42));

        System.out.println(
                "P7 still exists and is "
                        + topic.partition(7).lifecycleState());
    }
}
