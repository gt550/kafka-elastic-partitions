package io.kafkaelastic;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public final class TransactionState {
    private final String transactionId;
    private TransactionStatus status = TransactionStatus.OPEN;
    private final Set<TopicPartitionRef> participants = new LinkedHashSet<>();

    public TransactionState(String transactionId) {
        if (transactionId == null || transactionId.isBlank()) {
            throw new IllegalArgumentException("transactionId must not be blank");
        }
        this.transactionId = transactionId;
    }

    public String transactionId() { return transactionId; }
    public TransactionStatus status() { return status; }
    public boolean isOpen() { return status == TransactionStatus.OPEN; }

    public Set<TopicPartitionRef> participants() {
        return Collections.unmodifiableSet(participants);
    }

    public boolean hasParticipant(String topicName, int partitionId) {
        return participants.contains(new TopicPartitionRef(topicName, partitionId));
    }

    void addParticipant(String topicName, int partitionId) {
        ensureOpen();
        participants.add(new TopicPartitionRef(topicName, partitionId));
    }

    void commit() {
        ensureOpen();
        status = TransactionStatus.COMMITTED;
    }

    void abort() {
        ensureOpen();
        status = TransactionStatus.ABORTED;
    }

    private void ensureOpen() {
        if (!isOpen()) {
            throw new IllegalStateException(
                    "Transaction " + transactionId + " is already " + status);
        }
    }

    public record TopicPartitionRef(String topicName, int partitionId) {
        public TopicPartitionRef {
            Objects.requireNonNull(topicName, "topicName");
            if (partitionId < 0) {
                throw new IllegalArgumentException("partitionId must be >= 0");
            }
        }
    }
}
