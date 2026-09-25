package io.kafkaelastic;

public enum ProduceDecision {
    ACCEPTED,
    REJECTED_NOT_WRITABLE,
    REJECTED_UNKNOWN_PARTITION
}
