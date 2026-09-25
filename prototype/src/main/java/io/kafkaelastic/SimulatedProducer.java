package io.kafkaelastic;

import java.util.Objects;

public final class SimulatedProducer {

    private final TopicState topic;
    private final ElasticPartitioner partitioner;
    private final BrokerWriteGate brokerWriteGate;
    private ProducerMetadata cachedMetadata;

    public SimulatedProducer(
            TopicState topic,
            ElasticPartitioner partitioner,
            BrokerWriteGate brokerWriteGate) {

        this.topic = Objects.requireNonNull(topic, "topic");
        this.partitioner = Objects.requireNonNull(partitioner, "partitioner");
        this.brokerWriteGate =
                Objects.requireNonNull(brokerWriteGate, "brokerWriteGate");

        refreshMetadata();
    }

    public ProducerMetadata cachedMetadata() {
        return cachedMetadata;
    }

    public void refreshMetadata() {
        this.cachedMetadata = ProducerMetadata.capture(topic);
    }

    public int selectPartition(Object key) {
        return partitioner.selectPartition(key, cachedMetadata);
    }

    public ProducerSendResult send(Object key) {
        int firstPartition = selectPartition(key);
        ProduceDecision firstDecision =
                brokerWriteGate.evaluate(topic, firstPartition);

        if (firstDecision == ProduceDecision.ACCEPTED) {
            return new ProducerSendResult(
                    firstPartition,
                    firstDecision,
                    false,
                    firstPartition,
                    firstDecision);
        }

        if (firstDecision != ProduceDecision.REJECTED_NOT_WRITABLE) {
            return new ProducerSendResult(
                    firstPartition,
                    firstDecision,
                    false,
                    firstPartition,
                    firstDecision);
        }

        refreshMetadata();

        int retryPartition = selectPartition(key);
        ProduceDecision retryDecision =
                brokerWriteGate.evaluate(topic, retryPartition);

        return new ProducerSendResult(
                firstPartition,
                firstDecision,
                true,
                retryPartition,
                retryDecision);
    }

    public ProduceDecision sendToPartition(int partitionId) {
        return brokerWriteGate.evaluate(topic, partitionId);
    }
}
