package io.kafkaelastic;

public enum PartitionDeleteDecision {
    DELETED,
    REJECTED_STALE_EPOCH,
    REJECTED_NOT_RETIRED,
    REJECTED_DEADLINE_NOT_REACHED,
    REJECTED_UNKNOWN_PARTITION
}
