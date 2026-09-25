package io.kafkaelastic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class ProducerRoutingTest {

    private static final Instant START =
            Instant.parse("2026-09-25T15:00:00Z");

    private ResizeController resizeController(Clock clock) {
        return new ResizeController(
                clock,
                Duration.ofMinutes(5),
                Duration.ofDays(30));
    }

    @Test
    void refreshedProducerSelectsOnlyActivePartitions() {
        Clock clock = Clock.fixed(START, ZoneOffset.UTC);
        TopicState topic = new TopicState("orders", 10);
        ResizeController controller = resizeController(clock);

        controller.shrink(topic, 5);

        SimulatedProducer producer = new SimulatedProducer(
                topic,
                new ElasticPartitioner(),
                new BrokerWriteGate(clock));

        assertEquals(5, producer.cachedMetadata().writablePartitions().size());

        for (int key = 0; key < 100; key++) {
            int partition = producer.selectPartition(key);
            assertTrue(partition >= 0 && partition <= 4);
        }
    }

    @Test
    void staleProducerCanWriteTransitioningPartitionDuringGrace() {
        Clock clock = Clock.fixed(START, ZoneOffset.UTC);
        TopicState topic = new TopicState("orders", 10);

        SimulatedProducer producer = new SimulatedProducer(
                topic,
                new ElasticPartitioner(),
                new BrokerWriteGate(clock));

        ResizeController controller = resizeController(clock);
        controller.shrink(topic, 5);

        assertEquals(7, producer.selectPartition(7));

        ProducerSendResult result = producer.send(7);

        assertEquals(7, result.firstPartition());
        assertEquals(ProduceDecision.ACCEPTED, result.firstDecision());
        assertFalse(result.metadataRefreshed());
        assertTrue(result.accepted());
    }

    @Test
    void staleProducerRefreshesAndReroutesAfterTransitionGrace() {
        TopicState topic = new TopicState("orders", 10);

        Clock initialClock = Clock.fixed(START, ZoneOffset.UTC);

        SimulatedProducer staleProducer = new SimulatedProducer(
                topic,
                new ElasticPartitioner(),
                new BrokerWriteGate(
                        Clock.fixed(
                                START.plus(Duration.ofMinutes(6)),
                                ZoneOffset.UTC)));

        ResizeController controller = resizeController(initialClock);
        controller.shrink(topic, 5);

        assertEquals(7, staleProducer.selectPartition(7));

        ProducerSendResult result = staleProducer.send(7);

        assertEquals(7, result.firstPartition());
        assertEquals(
                ProduceDecision.REJECTED_NOT_WRITABLE,
                result.firstDecision());
        assertTrue(result.metadataRefreshed());
        assertTrue(result.finalPartition() >= 0 && result.finalPartition() <= 4);
        assertNotEquals(7, result.finalPartition());
        assertEquals(ProduceDecision.ACCEPTED, result.finalDecision());
        assertTrue(result.accepted());
    }

    @Test
    void brokerRejectsExplicitWriteToRetiredPartition() {
        TopicState topic = new TopicState("orders", 10);

        ResizeController startController =
                resizeController(Clock.fixed(START, ZoneOffset.UTC));

        startController.shrink(topic, 5);

        Clock afterGrace =
                Clock.fixed(
                        START.plus(Duration.ofMinutes(6)),
                        ZoneOffset.UTC);

        ResizeController afterGraceController =
                resizeController(afterGrace);

        afterGraceController.completeTransition(topic);

        SimulatedProducer producer = new SimulatedProducer(
                topic,
                new ElasticPartitioner(),
                new BrokerWriteGate(afterGrace));

        assertEquals(
                PartitionLifecycleState.RETIRED,
                topic.partition(7).lifecycleState());

        assertEquals(
                ProduceDecision.REJECTED_NOT_WRITABLE,
                producer.sendToPartition(7));
    }

    @Test
    void producerUsesReactivatedPartitionsAfterMetadataRefresh() {
        TopicState topic = new TopicState("orders", 10);

        ResizeController startController =
                resizeController(Clock.fixed(START, ZoneOffset.UTC));

        startController.shrink(topic, 5);

        Clock afterGrace =
                Clock.fixed(
                        START.plus(Duration.ofMinutes(6)),
                        ZoneOffset.UTC);

        ResizeController afterGraceController =
                resizeController(afterGrace);

        afterGraceController.completeTransition(topic);
        afterGraceController.expand(topic, 10);

        SimulatedProducer producer = new SimulatedProducer(
                topic,
                new ElasticPartitioner(),
                new BrokerWriteGate(afterGrace));

        assertEquals(10, producer.cachedMetadata().writablePartitions().size());
        assertEquals(7, producer.selectPartition(7));
        assertEquals(
                ProduceDecision.ACCEPTED,
                producer.sendToPartition(7));
    }
}
