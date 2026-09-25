package io.kafkaelastic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class FailureRecoveryTest {

    private static final Instant START =
            Instant.parse("2026-09-25T15:00:00Z");

    private ResizeController controller(
            Instant now,
            TransactionRegistry transactions,
            MetadataQuorum quorum) {

        return new ResizeController(
                Clock.fixed(now, ZoneOffset.UTC),
                Duration.ofMinutes(5),
                Duration.ofDays(30),
                transactions,
                quorum);
    }

    @Test
    void replacementControllerResumesSameResizeEpoch() {
        TopicState topic = new TopicState("orders", 10);
        MetadataQuorum quorum = new MetadataQuorum(true);

        ResizeController controller1 =
                controller(START, null, quorum);

        long epoch = controller1.shrink(topic, 5);

        ResizeController replacement =
                controller(
                        START.plus(Duration.ofMinutes(6)),
                        null,
                        quorum);

        replacement.completeTransition(topic);

        assertEquals(1L, epoch);
        assertEquals(1L, topic.currentResizeEpoch());
        assertEquals(
                PartitionLifecycleState.RETIRED,
                topic.partition(7).lifecycleState());
    }

    @Test
    void brokerLeaderReplacementPreservesLifecycleEnforcement() {
        TopicState topic = new TopicState("orders", 10);
        MetadataQuorum quorum = new MetadataQuorum(true);

        controller(START, null, quorum).shrink(topic, 5);

        Clock afterGrace =
                Clock.fixed(
                        START.plus(Duration.ofMinutes(6)),
                        ZoneOffset.UTC);

        BrokerWriteGate replacementLeader =
                new BrokerWriteGate(afterGrace);

        assertEquals(
                ProduceDecision.REJECTED_NOT_WRITABLE,
                replacementLeader.evaluate(topic, 7));

        assertEquals(
                PartitionLifecycleState.TRANSITIONING,
                topic.partition(7).lifecycleState());
    }

    @Test
    void quorumOutageFreezesTransitionEvenAfterDeadline() {
        TopicState topic = new TopicState("orders", 10);
        MetadataQuorum quorum = new MetadataQuorum(true);

        controller(START, null, quorum).shrink(topic, 5);

        quorum.setAvailable(false);

        ResizeController afterDeadline =
                controller(
                        START.plus(Duration.ofMinutes(6)),
                        null,
                        quorum);

        assertThrows(
                ControllerQuorumUnavailableException.class,
                () -> afterDeadline.completeTransition(topic));

        assertEquals(5L, topic.transitioningPartitionCount());
        assertEquals(0L, topic.retiredPartitionCount());
    }

    @Test
    void transitionContinuesAfterQuorumRecovery() {
        TopicState topic = new TopicState("orders", 10);
        MetadataQuorum quorum = new MetadataQuorum(true);

        controller(START, null, quorum).shrink(topic, 5);

        quorum.setAvailable(false);

        ResizeController afterDeadline =
                controller(
                        START.plus(Duration.ofMinutes(6)),
                        null,
                        quorum);

        assertThrows(
                ControllerQuorumUnavailableException.class,
                () -> afterDeadline.completeTransition(topic));

        quorum.setAvailable(true);
        afterDeadline.completeTransition(topic);

        assertEquals(0L, topic.transitioningPartitionCount());
        assertEquals(5L, topic.retiredPartitionCount());
    }

    @Test
    void partialRetirementCanResumeIdempotentlyAfterTransactionResolves() {
        TopicState topic = new TopicState("orders", 10);
        MetadataQuorum quorum = new MetadataQuorum(true);
        TransactionRegistry transactions = new TransactionRegistry();

        transactions.begin("T100");
        transactions.addParticipant("T100", topic, 7);

        controller(START, transactions, quorum).shrink(topic, 5);

        ResizeController afterGrace =
                controller(
                        START.plus(Duration.ofMinutes(6)),
                        transactions,
                        quorum);

        assertThrows(
                ResizeException.class,
                () -> afterGrace.completeTransition(topic));

        // P9 and P8 are processed first and may already be RETIRED.
        assertTrue(topic.retiredPartitionCount() > 0);
        assertEquals(
                PartitionLifecycleState.TRANSITIONING,
                topic.partition(7).lifecycleState());

        transactions.commit("T100");

        afterGrace.completeTransition(topic);
        afterGrace.completeTransition(topic); // duplicate retry is harmless

        assertEquals(5L, topic.retiredPartitionCount());
        assertEquals(0L, topic.transitioningPartitionCount());
    }

    @Test
    void staleDeleteCannotRemoveReactivatedPartition() {
        TopicState topic = new TopicState("orders", 10);
        MetadataQuorum quorum = new MetadataQuorum(true);

        ResizeController startController =
                controller(START, null, quorum);

        long oldEpoch = startController.shrink(topic, 5);

        ResizeController afterGrace =
                controller(
                        START.plus(Duration.ofMinutes(6)),
                        null,
                        quorum);

        afterGrace.completeTransition(topic);
        long newEpoch = afterGrace.expand(topic, 10);

        assertEquals(2L, newEpoch);
        assertEquals(
                PartitionLifecycleState.ACTIVE,
                topic.partition(7).lifecycleState());

        ResizeController day31 =
                controller(
                        START.plus(Duration.ofDays(31)),
                        null,
                        quorum);

        assertEquals(
                PartitionDeleteDecision.REJECTED_STALE_EPOCH,
                day31.deleteRetiredPartition(
                        topic, 7, oldEpoch));

        assertEquals(
                PartitionLifecycleState.ACTIVE,
                topic.partition(7).lifecycleState());
    }

    @Test
    void retiredPartitionCanBePhysicallyDeletedAfterDeadline() {
        TopicState topic = new TopicState("orders", 10);
        MetadataQuorum quorum = new MetadataQuorum(true);

        long epoch =
                controller(START, null, quorum).shrink(topic, 5);

        controller(
                START.plus(Duration.ofMinutes(6)),
                null,
                quorum)
                .completeTransition(topic);

        ResizeController day31 =
                controller(
                        START.plus(Duration.ofDays(31)),
                        null,
                        quorum);

        assertEquals(
                PartitionDeleteDecision.DELETED,
                day31.deleteRetiredPartition(topic, 7, epoch));

        assertEquals(9L, topic.physicalPartitionCount());

        assertThrows(
                IllegalArgumentException.class,
                () -> topic.partition(7));
    }

    @Test
    void deletionBeforeDeadlineIsRejected() {
        TopicState topic = new TopicState("orders", 10);
        MetadataQuorum quorum = new MetadataQuorum(true);

        long epoch =
                controller(START, null, quorum).shrink(topic, 5);

        controller(
                START.plus(Duration.ofMinutes(6)),
                null,
                quorum)
                .completeTransition(topic);

        ResizeController day10 =
                controller(
                        START.plus(Duration.ofDays(10)),
                        null,
                        quorum);

        assertEquals(
                PartitionDeleteDecision.REJECTED_DEADLINE_NOT_REACHED,
                day10.deleteRetiredPartition(topic, 7, epoch));
    }

    @Test
    void deletedPartitionIdIsNotReusedByFutureExpansion() {
        TopicState topic = new TopicState("orders", 10);
        MetadataQuorum quorum = new MetadataQuorum(true);

        long epoch =
                controller(START, null, quorum).shrink(topic, 5);

        controller(
                START.plus(Duration.ofMinutes(6)),
                null,
                quorum)
                .completeTransition(topic);

        ResizeController day31 =
                controller(
                        START.plus(Duration.ofDays(31)),
                        null,
                        quorum);

        // Delete all retired partitions P5-P9.
        for (int p = 5; p <= 9; p++) {
            assertEquals(
                    PartitionDeleteDecision.DELETED,
                    day31.deleteRetiredPartition(
                            topic, p, epoch));
        }

        assertEquals(5L, topic.physicalPartitionCount());

        day31.expand(topic, 6);

        // P5-P9 represented deleted physical instances and are not reused.
        assertEquals(
                PartitionLifecycleState.ACTIVE,
                topic.partition(10).lifecycleState());
    }
}
