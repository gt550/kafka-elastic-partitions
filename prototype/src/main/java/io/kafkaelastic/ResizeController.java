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
    private final TransactionRegistry transactionRegistry;
    private final MetadataQuorum metadataQuorum;

    public ResizeController(
            Clock clock,
            Duration transitionGrace,
            Duration retirementDeleteDelay) {
        this(
                clock,
                transitionGrace,
                retirementDeleteDelay,
                null,
                new MetadataQuorum(true));
    }

    public ResizeController(
            Clock clock,
            Duration transitionGrace,
            Duration retirementDeleteDelay,
            TransactionRegistry transactionRegistry) {
        this(
                clock,
                transitionGrace,
                retirementDeleteDelay,
                transactionRegistry,
                new MetadataQuorum(true));
    }

    public ResizeController(
            Clock clock,
            Duration transitionGrace,
            Duration retirementDeleteDelay,
            TransactionRegistry transactionRegistry,
            MetadataQuorum metadataQuorum) {

        this.clock = clock;
        this.transitionGrace = transitionGrace;
        this.retirementDeleteDelay = retirementDeleteDelay;
        this.transactionRegistry = transactionRegistry;
        this.metadataQuorum = metadataQuorum;
    }

    public long resize(TopicState topic, int targetActivePartitionCount) {
        requireQuorum();

        if (topic.resizeInProgress()) {
            throw new ResizeInProgressException(
                    "Resize already in progress for topic "
                            + topic.topicName());
        }

        int currentActive = Math.toIntExact(topic.activePartitionCount());

        if (targetActivePartitionCount <= 0) {
            throw new IllegalArgumentException(
                    "targetActivePartitionCount must be > 0");
        }

        if (targetActivePartitionCount == currentActive) {
            return topic.currentResizeEpoch();
        }

        return targetActivePartitionCount < currentActive
                ? shrink(topic, targetActivePartitionCount)
                : expand(topic, targetActivePartitionCount);
    }

    public long shrink(TopicState topic, int targetActivePartitionCount) {
        requireQuorum();

        int currentActive = Math.toIntExact(topic.activePartitionCount());

        if (targetActivePartitionCount <= 0
                || targetActivePartitionCount >= currentActive) {
            throw new IllegalArgumentException(
                    "Shrink target must be between 1 and currentActive - 1");
        }

        long epoch = topic.beginResize(targetActivePartitionCount);
        Instant deadline = clock.instant().plus(transitionGrace);

        List<PartitionState> toTransition = topic.activePartitions().stream()
                .sorted(
                        Comparator.comparingInt(
                                PartitionState::partitionId).reversed())
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
        requireQuorum();

        if (!topic.resizeInProgress()) {
            return;
        }

        long epoch = topic.currentResizeEpoch();
        Instant now = clock.instant();

        List<PartitionState> transitioning = topic.partitions().stream()
                .filter(
                        p -> p.lifecycleState()
                                == PartitionLifecycleState.TRANSITIONING)
                .toList();

        for (PartitionState partition : transitioning) {
            Instant deadline = partition.transitionDeadline();

            if (deadline != null && now.isBefore(deadline)) {
                throw new ResizeException(
                        "Transition grace has not expired for partition "
                                + partition.partitionId());
            }

            if (transactionRegistry != null
                    && transactionRegistry.hasOpenTransactionForPartition(
                            topic.topicName(),
                            partition.partitionId())) {

                throw new ResizeException(
                        "Partition " + partition.partitionId()
                                + " still has "
                                + transactionRegistry
                                        .openTransactionCountForPartition(
                                                topic.topicName(),
                                                partition.partitionId())
                                + " open transaction(s)");
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

    public long expand(
            TopicState topic,
            int targetActivePartitionCount) {

        requireQuorum();

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
            retired.get(i).transitionTo(
                    PartitionLifecycleState.ACTIVE,
                    epoch,
                    null,
                    null);
        }

        needed -= reactivateCount;

        for (int i = 0; i < needed; i++) {
            PartitionState partition =
                    new PartitionState(topic.allocatePartitionId());

            partition.transitionTo(
                    PartitionLifecycleState.ACTIVE,
                    epoch,
                    null,
                    null);

            topic.addPartition(partition);
        }

        topic.completeResize();
        return epoch;
    }

    /**
     * Epoch-fenced physical deletion of a retired partition.
     */
    public PartitionDeleteDecision deleteRetiredPartition(
            TopicState topic,
            int partitionId,
            long requestedEpoch) {

        requireQuorum();

        final PartitionState partition;

        try {
            partition = topic.partition(partitionId);
        } catch (IllegalArgumentException e) {
            return PartitionDeleteDecision.REJECTED_UNKNOWN_PARTITION;
        }

        if (requestedEpoch < partition.lifecycleEpoch()) {
            return PartitionDeleteDecision.REJECTED_STALE_EPOCH;
        }

        if (requestedEpoch > partition.lifecycleEpoch()) {
            throw new ResizeException(
                    "Delete epoch " + requestedEpoch
                            + " is ahead of partition lifecycle epoch "
                            + partition.lifecycleEpoch());
        }

        if (partition.lifecycleState()
                != PartitionLifecycleState.RETIRED) {
            return PartitionDeleteDecision.REJECTED_NOT_RETIRED;
        }

        Instant deadline = partition.retirementDeleteDeadline();

        if (deadline == null || clock.instant().isBefore(deadline)) {
            return PartitionDeleteDecision.REJECTED_DEADLINE_NOT_REACHED;
        }

        topic.removePartition(partitionId);
        return PartitionDeleteDecision.DELETED;
    }

    public void assertCurrentEpoch(
            PartitionState partition,
            long requestedEpoch) {

        if (requestedEpoch < partition.lifecycleEpoch()) {
            throw new StaleResizeEpochException(
                    "Partition " + partition.partitionId()
                            + " is owned by epoch "
                            + partition.lifecycleEpoch()
                            + "; stale epoch "
                            + requestedEpoch
                            + " rejected");
        }
    }

    private void requireQuorum() {
        if (!metadataQuorum.isAvailable()) {
            throw new ControllerQuorumUnavailableException(
                    "Metadata quorum is unavailable; lifecycle state is frozen");
        }
    }
}
