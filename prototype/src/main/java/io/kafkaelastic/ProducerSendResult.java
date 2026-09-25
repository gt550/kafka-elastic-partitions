package io.kafkaelastic;

public record ProducerSendResult(
        int firstPartition,
        ProduceDecision firstDecision,
        boolean metadataRefreshed,
        int finalPartition,
        ProduceDecision finalDecision) {

    public boolean accepted() {
        return finalDecision == ProduceDecision.ACCEPTED;
    }
}
