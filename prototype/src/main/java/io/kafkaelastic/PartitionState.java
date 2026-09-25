package io.kafkaelastic;

import java.time.Instant;
import java.util.Objects;

public final class PartitionState {

    private final int partitionId;
    private PartitionLifecycleState lifecycleState;
    private long lifecycleEpoch;
    private Instant transitionDeadline;
    private Instant retirementDeleteDeadline;

    public PartitionState(int partitionId) {
        if (partitionId < 0) {
            throw new IllegalArgumentException("partitionId must be >= 0");
        }

        this.partitionId = partitionId;
        this.lifecycleState = PartitionLifecycleState.ACTIVE;
        this.lifecycleEpoch = 0L;
    }

    public int partitionId() {
        return partitionId;
    }

    public PartitionLifecycleState lifecycleState() {
        return lifecycleState;
    }

    public long lifecycleEpoch() {
        return lifecycleEpoch;
    }

    public Instant transitionDeadline() {
        return transitionDeadline;
    }

    public Instant retirementDeleteDeadline() {
        return retirementDeleteDeadline;
    }

    void transitionTo(
            PartitionLifecycleState newState,
            long resizeEpoch,
            Instant transitionDeadline,
            Instant retirementDeleteDeadline) {

        Objects.requireNonNull(newState, "newState");

        if (resizeEpoch < lifecycleEpoch) {
            throw new StaleResizeEpochException(
                    "Partition " + partitionId
                            + " is owned by epoch " + lifecycleEpoch
                            + "; received stale epoch " + resizeEpoch);
        }

        validateTransition(lifecycleState, newState);

        this.lifecycleState = newState;
        this.lifecycleEpoch = resizeEpoch;
        this.transitionDeadline = transitionDeadline;
        this.retirementDeleteDeadline = retirementDeleteDeadline;
    }

    private static void validateTransition(
            PartitionLifecycleState current,
            PartitionLifecycleState next) {

        if (current == next) {
            return;
        }

        boolean valid =
                (current == PartitionLifecycleState.ACTIVE
                        && next == PartitionLifecycleState.TRANSITIONING)
                || (current == PartitionLifecycleState.TRANSITIONING
                        && next == PartitionLifecycleState.RETIRED)
                || (current == PartitionLifecycleState.RETIRED
                        && next == PartitionLifecycleState.ACTIVE);

        if (!valid) {
            throw new IllegalStateException(
                    "Invalid lifecycle transition: " + current + " -> " + next);
        }
    }

    @Override
    public String toString() {
        return "P" + partitionId
                + "{state=" + lifecycleState
                + ", epoch=" + lifecycleEpoch
                + '}';
    }
}
