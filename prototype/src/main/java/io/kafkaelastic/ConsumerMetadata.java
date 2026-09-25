package io.kafkaelastic;

import java.util.Comparator;
import java.util.List;

/**
 * Immutable consumer-side metadata snapshot.
 *
 * Unlike producer metadata, every existing physical partition is readable:
 * ACTIVE + TRANSITIONING + RETIRED.
 */
public record ConsumerMetadata(
        String topicName,
        long observedResizeEpoch,
        List<PartitionView> partitions) {

    public ConsumerMetadata {
        partitions = List.copyOf(partitions);
    }

    public static ConsumerMetadata capture(TopicState topic) {
        List<PartitionView> snapshot = topic.partitions().stream()
                .sorted(Comparator.comparingInt(PartitionState::partitionId))
                .map(p -> new PartitionView(
                        p.partitionId(),
                        p.lifecycleState(),
                        p.lifecycleEpoch()))
                .toList();

        return new ConsumerMetadata(
                topic.topicName(),
                topic.currentResizeEpoch(),
                snapshot);
    }

    public List<PartitionView> readablePartitions() {
        return partitions.stream()
                .sorted(Comparator.comparingInt(PartitionView::partitionId))
                .toList();
    }

    public boolean isReadable(int partitionId) {
        return partitions.stream()
                .anyMatch(p -> p.partitionId() == partitionId);
    }

    public record PartitionView(
            int partitionId,
            PartitionLifecycleState lifecycleState,
            long lifecycleEpoch) {
    }
}
