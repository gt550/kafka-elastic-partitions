package io.kafkaelastic;

public enum TransactionParticipantDecision {
    ADDED,
    ALREADY_PARTICIPATING,
    REJECTED_PARTITION_NOT_ACTIVE,
    REJECTED_TRANSACTION_NOT_OPEN
}
