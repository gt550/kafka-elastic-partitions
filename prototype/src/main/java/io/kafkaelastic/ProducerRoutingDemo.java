package io.kafkaelastic;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

public final class ProducerRoutingDemo {

    public static void main(String[] args) {
        Instant start = Instant.parse("2026-09-25T15:00:00Z");

        TopicState topic = new TopicState("orders", 10);

        SimulatedProducer staleProducer = new SimulatedProducer(
                topic,
                new ElasticPartitioner(),
                new BrokerWriteGate(
                        Clock.fixed(
                                start.plus(Duration.ofMinutes(6)),
                                ZoneOffset.UTC)));

        System.out.println(
                "Cached producer writable partitions before resize: "
                        + staleProducer.cachedMetadata()
                                .writablePartitions()
                                .stream()
                                .map(ProducerMetadata.PartitionView::partitionId)
                                .toList());

        ResizeController controller = new ResizeController(
                Clock.fixed(start, ZoneOffset.UTC),
                Duration.ofMinutes(5),
                Duration.ofDays(30));

        controller.shrink(topic, 5);

        System.out.println(
                "Stale producer initially selects P"
                        + staleProducer.selectPartition(7));

        ProducerSendResult result = staleProducer.send(7);

        System.out.println("First decision: " + result.firstDecision());
        System.out.println("Metadata refreshed: " + result.metadataRefreshed());
        System.out.println("Retry partition: P" + result.finalPartition());
        System.out.println("Final decision: " + result.finalDecision());
    }
}
