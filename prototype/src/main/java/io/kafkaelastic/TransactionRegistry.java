package io.kafkaelastic;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class TransactionRegistry {
    private final Map<String, TransactionState> transactions =
            new LinkedHashMap<>();

    public TransactionState begin(String transactionId) {
        if (transactions.containsKey(transactionId)) {
            throw new IllegalArgumentException(
                    "Transaction already exists: " + transactionId);
        }
        TransactionState tx = new TransactionState(transactionId);
        transactions.put(transactionId, tx);
        return tx;
    }

    public TransactionState transaction(String transactionId) {
        TransactionState tx = transactions.get(transactionId);
        if (tx == null) {
            throw new IllegalArgumentException(
                    "Unknown transaction: " + transactionId);
        }
        return tx;
    }

    public TransactionParticipantDecision addParticipant(
            String transactionId, TopicState topic, int partitionId) {

        Objects.requireNonNull(topic, "topic");
        TransactionState tx = transaction(transactionId);

        if (!tx.isOpen()) {
            return TransactionParticipantDecision.REJECTED_TRANSACTION_NOT_OPEN;
        }

        if (tx.hasParticipant(topic.topicName(), partitionId)) {
            return TransactionParticipantDecision.ALREADY_PARTICIPATING;
        }

        PartitionState partition = topic.partition(partitionId);
        if (partition.lifecycleState() != PartitionLifecycleState.ACTIVE) {
            return TransactionParticipantDecision.REJECTED_PARTITION_NOT_ACTIVE;
        }

        tx.addParticipant(topic.topicName(), partitionId);
        return TransactionParticipantDecision.ADDED;
    }

    public void commit(String transactionId) {
        transaction(transactionId).commit();
    }

    public void abort(String transactionId) {
        transaction(transactionId).abort();
    }

    public boolean hasOpenTransactionForPartition(
            String topicName, int partitionId) {
        return transactions.values().stream()
                .filter(TransactionState::isOpen)
                .anyMatch(tx -> tx.hasParticipant(topicName, partitionId));
    }

    public long openTransactionCountForPartition(
            String topicName, int partitionId) {
        return transactions.values().stream()
                .filter(TransactionState::isOpen)
                .filter(tx -> tx.hasParticipant(topicName, partitionId))
                .count();
    }

    public Collection<TransactionState> transactions() {
        return java.util.List.copyOf(transactions.values());
    }
}
