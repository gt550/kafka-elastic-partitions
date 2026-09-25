package io.kafkaelastic;

import java.util.List;
import java.util.Objects;

/**
 * Simplified consumer using cached readable-partition metadata.
 */
public final class SimulatedConsumer {

    private final TopicState topic;
    private final ConsumerGroupState group;
    private ConsumerMetadata cachedMetadata;

    public SimulatedConsumer(
            TopicState topic,
            ConsumerGroupState group) {

        this.topic = Objects.requireNonNull(topic, "topic");
        this.group = Objects.requireNonNull(group, "group");
        refreshMetadata();
    }

    public ConsumerGroupState group() {
        return group;
    }

    public ConsumerMetadata cachedMetadata() {
        return cachedMetadata;
    }

    public void refreshMetadata() {
        this.cachedMetadata = ConsumerMetadata.capture(topic);
    }

    public List<Integer> readablePartitionIds() {
        return cachedMetadata.readablePartitions().stream()
                .map(ConsumerMetadata.PartitionView::partitionId)
                .toList();
    }

    public boolean canRead(int partitionId) {
        return cachedMetadata.isReadable(partitionId);
    }

    public void commit(int partitionId, long offset) {
        if (!canRead(partitionId)) {
            throw new IllegalArgumentException(
                    "Partition " + partitionId + " is not readable");
        }

        group.commit(partitionId, offset);
    }
}
