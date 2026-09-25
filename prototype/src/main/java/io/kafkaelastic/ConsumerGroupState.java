package io.kafkaelastic;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Minimal consumer-group model for the prototype.
 *
 * committedOffsets represent durable Kafka group offsets.
 */
public final class ConsumerGroupState {

    private final String groupId;
    private final String topicName;
    private boolean activelySubscribed;
    private final Map<Integer, Long> committedOffsets = new HashMap<>();

    public ConsumerGroupState(
            String groupId,
            String topicName,
            boolean activelySubscribed) {

        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("groupId must not be blank");
        }

        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("topicName must not be blank");
        }

        this.groupId = groupId;
        this.topicName = topicName;
        this.activelySubscribed = activelySubscribed;
    }

    public String groupId() {
        return groupId;
    }

    public String topicName() {
        return topicName;
    }

    public boolean activelySubscribed() {
        return activelySubscribed;
    }

    public void setActivelySubscribed(boolean activelySubscribed) {
        this.activelySubscribed = activelySubscribed;
    }

    public void commit(int partitionId, long offset) {
        if (partitionId < 0) {
            throw new IllegalArgumentException("partitionId must be >= 0");
        }

        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }

        committedOffsets.put(partitionId, offset);
    }

    public Long committedOffset(int partitionId) {
        return committedOffsets.get(partitionId);
    }

    public Map<Integer, Long> committedOffsets() {
        return Collections.unmodifiableMap(committedOffsets);
    }

    public boolean isRelevantToTopic(String topic) {
        return Objects.equals(topicName, topic)
                && (activelySubscribed || !committedOffsets.isEmpty());
    }
}
