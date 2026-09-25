package io.kafkaelastic;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;

public final class ResizeController {

    private final Clock clock;
    private final Duration transitionGrace;
    private final Duration retirementDeleteDelay;

    public ResizeController(
            Clock clock,
            Duration transitionGrace,
            Duration retirementDeleteDelay) {

        this.clock = clock;
        this.transitionGrace = transitionGrace;
        this.retirementDeleteDelay = retirementDeleteDelay;
    }

    public long resize(TopicState topic, int targetActivePartitionCount) {
        int currentActive = Math.toIntExact(topic.activePartitionCount());

        if (targetActivePartitionCount <= 0) {
            throw new IllegalArgumentException(
                    "targetActivePartitionCount must be > 0");
        }

        if (targetActivePartitionCount == currentActive) {
            return topic.currentResizeEpoch();
        }

        if (targetActivePartitionCount < currentActive) {
            return shrink(topic, targetActivePartitionCount);
        }

        return expand(topic, targetActivePartitionCount);
    }

    public long shrink(TopicState topic, int targetActivePartitionCount) {
        int currentActive = Math.toIntExact(topic.activePartitionCount());

        if (targetActivePartitionCount <= 0
                || targetActivePartitionCount >= currentActive) {
            throw new IllegalArgumentException(
                    "Shrink target must be between 1 and currentActive - 1");
        }

        long epoch = topic.beginResize(targetActivePartitionCount);
        Instant deadline = clock.instant().plus(transitionGrace);

        List<PartitionState> toTransition = topic.activePartitions().stream()
                .sorted(Comparator.comparingInt(PartitionState::partitionId).reversed())
                .limit(currentActive - targetActivePartitionCount)
                .toList();

        for (PartitionState partition : toTransition) {
            partition.transitionTo(
                    PartitionLifecycleState.TRANSITIONING,
                    epoch,
                    deadline,
                    null);
        }

        return epoch;
    }

    public void completeTransition(TopicState topic) {
        if (!topic.resizeInProgress()) {
            return;
        }

        long epoch = topic.currentResizeEpoch();
        Instant now = clock.instant();

        List<PartitionState> transitioning = topic.partitions().stream()
                .filter(p -> p.lifecycleState() == PartitionLifecycleState.TRANSITIONING)
                .toList();

        for (PartitionState partition : transitioning) {
            Instant deadline = partition.transitionDeadline();

            if (deadline != null && now.isBefore(deadline)) {
                throw new ResizeException(
                        "Transition grace has not expired for partition "
                                + partition.partitionId());
            }

            partition.transitionTo(
                    PartitionLifecycleState.RETIRED,
                    epoch,
                    null,
                    now.plus(retirementDeleteDelay));
        }

        if (topic.transitioningPartitionCount() == 0
                && topic.activePartitionCount()
                        == topic.targetActivePartitionCount()) {
            topic.completeResize();
        }
    }

    public long expand(TopicState topic, int targetActivePartitionCount) {
        int currentActive = Math.toIntExact(topic.activePartitionCount());

        if (targetActivePartitionCount <= currentActive) {
            throw new IllegalArgumentException(
                    "Expansion target must be greater than current active count");
        }

        long epoch = topic.beginResize(targetActivePartitionCount);
        int needed = targetActivePartitionCount - currentActive;

        List<PartitionState> retired = topic.retiredPartitions();

        int reactivateCount = Math.min(needed, retired.size());

        for (int i = 0; i < reactivateCount; i++) {
            PartitionState partition = retired.get(i);
            partition.transitionTo(
                    PartitionLifecycleState.ACTIVE,
                    epoch,
                    null,
                    null);
        }

        needed -= reactivateCount;

        if (needed > 0) {
            int nextPartitionId = topic.partitions().stream()
                    .mapToInt(PartitionState::partitionId)
                    .max()
                    .orElse(-1) + 1;

            for (int i = 0; i < needed; i++) {
                PartitionState partition = new PartitionState(nextPartitionId + i);
                partition.transitionTo(
                        PartitionLifecycleState.ACTIVE,
                        epoch,
                        null,
                        null);
                topic.addPartition(partition);
            }
        }

        topic.completeResize();
        return epoch;
    }

    public void assertCurrentEpoch(PartitionState partition, long requestedEpoch) {
        if (requestedEpoch < partition.lifecycleEpoch()) {
            throw new StaleResizeEpochException(
                    "Partition " + partition.partitionId()
                            + " is owned by epoch " + partition.lifecycleEpoch()
                            + "; stale epoch " + requestedEpoch + " rejected");
        }
    }
}
