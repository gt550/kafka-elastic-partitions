package io.kafkaelastic;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Operational retirement tracking.
 *
 * Readability and drain tracking are intentionally separate:
 * a consumer may read a RETIRED partition without being allowed to extend
 * the retirement lifecycle.
 */
public final class DrainTracker {

    private final String topicName;
    private final long retirementResizeEpoch;
    private final Map<String, ConsumerGroupState> trackedGroups =
            new LinkedHashMap<>();

    public DrainTracker(
            String topicName,
            long retirementResizeEpoch,
            Collection<ConsumerGroupState> groupsPresentAtRetirement) {

        this.topicName = topicName;
        this.retirementResizeEpoch = retirementResizeEpoch;

        for (ConsumerGroupState group : groupsPresentAtRetirement) {
            if (group.isRelevantToTopic(topicName)) {
                trackedGroups.put(group.groupId(), group);
            }
        }
    }

    public String topicName() {
        return topicName;
    }

    public long retirementResizeEpoch() {
        return retirementResizeEpoch;
    }

    public Set<String> trackedGroupIds() {
        return Set.copyOf(trackedGroups.keySet());
    }

    public boolean isTracked(String groupId) {
        return trackedGroups.containsKey(groupId);
    }

    public void addGroup(ConsumerGroupState group) {
        if (!group.topicName().equals(topicName)) {
            throw new IllegalArgumentException(
                    "Group belongs to a different topic");
        }

        trackedGroups.put(group.groupId(), group);
    }

    public void removeGroup(String groupId) {
        trackedGroups.remove(groupId);
    }

    /**
     * endOffset is the partition's current LEO / required durable position.
     *
     * For a RETIRED partition this value no longer increases from application
     * writes, so no separate retirementEndOffset is required.
     */
    public DrainStatus status(
            String groupId,
            int partitionId,
            long endOffset) {

        ConsumerGroupState group = trackedGroups.get(groupId);

        if (group == null) {
            return DrainStatus.NOT_TRACKED;
        }

        Long committed = group.committedOffset(partitionId);

        if (committed == null) {
            return DrainStatus.NO_COMMITTED_OFFSET;
        }

        return committed >= endOffset
                ? DrainStatus.DRAINED
                : DrainStatus.LAGGING;
    }
}
