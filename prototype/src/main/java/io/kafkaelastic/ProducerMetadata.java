package io.kafkaelastic;

import java.util.Comparator;
import java.util.List;

public record ProducerMetadata(
        String topicName,
        long observedResizeEpoch,
        List<PartitionView> partitions) {

    public ProducerMetadata {
        partitions = List.copyOf(partitions);
    }

    public static ProducerMetadata capture(TopicState topic) {
        List<PartitionView> snapshot = topic.partitions().stream()
                .sorted(Comparator.comparingInt(PartitionState::partitionId))
                .map(p -> new PartitionView(
                        p.partitionId(),
                        p.lifecycleState(),
                        p.lifecycleEpoch()))
                .toList();

        return new ProducerMetadata(
                topic.topicName(),
                topic.currentResizeEpoch(),
                snapshot);
    }

    public List<PartitionView> writablePartitions() {
        return partitions.stream()
                .filter(p -> p.lifecycleState() == PartitionLifecycleState.ACTIVE)
                .sorted(Comparator.comparingInt(PartitionView::partitionId))
                .toList();
    }

    public record PartitionView(
            int partitionId,
            PartitionLifecycleState lifecycleState,
            long lifecycleEpoch) {
    }
}
