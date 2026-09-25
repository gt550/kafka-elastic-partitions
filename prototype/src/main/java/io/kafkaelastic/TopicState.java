package io.kafkaelastic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public final class TopicState {

    private final String topicName;
    private final List<PartitionState> partitions;

    private long currentResizeEpoch;
    private Integer targetActivePartitionCount;
    private boolean resizeInProgress;

    // Partition IDs are never reused after physical deletion.
    private int nextPartitionId;

    public TopicState(String topicName, int partitionCount) {
        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("topicName must not be blank");
        }

        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be > 0");
        }

        this.topicName = topicName;
        this.partitions = new ArrayList<>(partitionCount);

        for (int i = 0; i < partitionCount; i++) {
            partitions.add(new PartitionState(i));
        }

        this.nextPartitionId = partitionCount;
    }

    public String topicName() {
        return topicName;
    }

    public long currentResizeEpoch() {
        return currentResizeEpoch;
    }

    public Integer targetActivePartitionCount() {
        return targetActivePartitionCount;
    }

    public boolean resizeInProgress() {
        return resizeInProgress;
    }

    public List<PartitionState> partitions() {
        return Collections.unmodifiableList(partitions);
    }

    public PartitionState partition(int partitionId) {
        return partitions.stream()
                .filter(p -> p.partitionId() == partitionId)
                .findFirst()
                .orElseThrow(() ->
                        new IllegalArgumentException(
                                "Unknown partition: " + partitionId));
    }

    public long physicalPartitionCount() {
        return partitions.size();
    }

    public long activePartitionCount() {
        return partitions.stream()
                .filter(p -> p.lifecycleState() == PartitionLifecycleState.ACTIVE)
                .count();
    }

    public long transitioningPartitionCount() {
        return partitions.stream()
                .filter(p -> p.lifecycleState() == PartitionLifecycleState.TRANSITIONING)
                .count();
    }

    public long retiredPartitionCount() {
        return partitions.stream()
                .filter(p -> p.lifecycleState() == PartitionLifecycleState.RETIRED)
                .count();
    }

    public List<PartitionState> activePartitions() {
        return partitions.stream()
                .filter(p -> p.lifecycleState() == PartitionLifecycleState.ACTIVE)
                .sorted(Comparator.comparingInt(PartitionState::partitionId))
                .toList();
    }

    public List<PartitionState> retiredPartitions() {
        return partitions.stream()
                .filter(p -> p.lifecycleState() == PartitionLifecycleState.RETIRED)
                .sorted(Comparator.comparingInt(PartitionState::partitionId))
                .toList();
    }

    long beginResize(int targetActivePartitionCount) {
        if (resizeInProgress) {
            throw new ResizeInProgressException(
                    "Resize already in progress for topic " + topicName);
        }

        this.currentResizeEpoch++;
        this.targetActivePartitionCount = targetActivePartitionCount;
        this.resizeInProgress = true;

        return currentResizeEpoch;
    }

    void completeResize() {
        this.resizeInProgress = false;
    }

    int allocatePartitionId() {
        return nextPartitionId++;
    }

    void addPartition(PartitionState partitionState) {
        partitions.add(partitionState);
        nextPartitionId = Math.max(
                nextPartitionId,
                partitionState.partitionId() + 1);
    }

    void removePartition(int partitionId) {
        boolean removed =
                partitions.removeIf(p -> p.partitionId() == partitionId);

        if (!removed) {
            throw new IllegalArgumentException(
                    "Unknown partition: " + partitionId);
        }
    }
}
