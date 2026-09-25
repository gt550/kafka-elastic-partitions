package io.kafkaelastic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class TransactionSemanticsTest {

    private static final Instant START =
            Instant.parse("2026-09-25T15:00:00Z");

    private ResizeController controller(
            Clock clock,
            TransactionRegistry transactions) {
        return new ResizeController(
                clock,
                Duration.ofMinutes(5),
                Duration.ofDays(30),
                transactions);
    }

    @Test
    void existingParticipantMayContinueAfterTransitionStarts() {
        TopicState topic = new TopicState("orders", 10);
        TransactionRegistry txs = new TransactionRegistry();

        txs.begin("T100");
        assertEquals(TransactionParticipantDecision.ADDED,
                txs.addParticipant("T100", topic, 7));

        controller(Clock.fixed(START, ZoneOffset.UTC), txs)
                .shrink(topic, 5);

        assertEquals(TransactionParticipantDecision.ALREADY_PARTICIPATING,
                txs.addParticipant("T100", topic, 7));
    }

    @Test
    void transactionStartedEarlierCannotNewlyAddTransitioningPartition() {
        TopicState topic = new TopicState("orders", 10);
        TransactionRegistry txs = new TransactionRegistry();

        txs.begin("T100");
        txs.addParticipant("T100", topic, 2);

        controller(Clock.fixed(START, ZoneOffset.UTC), txs)
                .shrink(topic, 5);

        assertEquals(
                TransactionParticipantDecision.REJECTED_PARTITION_NOT_ACTIVE,
                txs.addParticipant("T100", topic, 7));
    }

    @Test
    void newTransactionCannotAddTransitioningPartition() {
        TopicState topic = new TopicState("orders", 10);
        TransactionRegistry txs = new TransactionRegistry();

        controller(Clock.fixed(START, ZoneOffset.UTC), txs)
                .shrink(topic, 5);

        txs.begin("T101");

        assertEquals(
                TransactionParticipantDecision.REJECTED_PARTITION_NOT_ACTIVE,
                txs.addParticipant("T101", topic, 7));
    }

    @Test
    void openExistingTransactionBlocksRetirement() {
        TopicState topic = new TopicState("orders", 10);
        TransactionRegistry txs = new TransactionRegistry();

        txs.begin("T100");
        txs.addParticipant("T100", topic, 7);

        controller(Clock.fixed(START, ZoneOffset.UTC), txs)
                .shrink(topic, 5);

        ResizeController afterGrace = controller(
                Clock.fixed(START.plus(Duration.ofMinutes(6)), ZoneOffset.UTC),
                txs);

        assertThrows(
                ResizeException.class,
                () -> afterGrace.completeTransition(topic));

        assertEquals(
                PartitionLifecycleState.TRANSITIONING,
                topic.partition(7).lifecycleState());
    }

    @Test
    void commitAllowsRetirement() {
        TopicState topic = new TopicState("orders", 10);
        TransactionRegistry txs = new TransactionRegistry();

        txs.begin("T100");
        txs.addParticipant("T100", topic, 7);

        controller(Clock.fixed(START, ZoneOffset.UTC), txs)
                .shrink(topic, 5);

        txs.commit("T100");

        controller(
                Clock.fixed(START.plus(Duration.ofMinutes(6)), ZoneOffset.UTC),
                txs)
                .completeTransition(topic);

        assertEquals(
                PartitionLifecycleState.RETIRED,
                topic.partition(7).lifecycleState());
    }

    @Test
    void abortAllowsRetirement() {
        TopicState topic = new TopicState("orders", 10);
        TransactionRegistry txs = new TransactionRegistry();

        txs.begin("T100");
        txs.addParticipant("T100", topic, 7);

        controller(Clock.fixed(START, ZoneOffset.UTC), txs)
                .shrink(topic, 5);

        txs.abort("T100");

        controller(
                Clock.fixed(START.plus(Duration.ofMinutes(6)), ZoneOffset.UTC),
                txs)
                .completeTransition(topic);

        assertEquals(
                PartitionLifecycleState.RETIRED,
                topic.partition(7).lifecycleState());
    }

    @Test
    void retiredPartitionCannotJoinNewTransaction() {
        TopicState topic = new TopicState("orders", 10);
        TransactionRegistry txs = new TransactionRegistry();

        controller(Clock.fixed(START, ZoneOffset.UTC), txs)
                .shrink(topic, 5);

        ResizeController afterGrace = controller(
                Clock.fixed(START.plus(Duration.ofMinutes(6)), ZoneOffset.UTC),
                txs);

        afterGrace.completeTransition(topic);

        txs.begin("T101");
        assertEquals(
                TransactionParticipantDecision.REJECTED_PARTITION_NOT_ACTIVE,
                txs.addParticipant("T101", topic, 7));
    }

    @Test
    void reactivatedPartitionCanJoinNewTransaction() {
        TopicState topic = new TopicState("orders", 10);
        TransactionRegistry txs = new TransactionRegistry();

        controller(Clock.fixed(START, ZoneOffset.UTC), txs)
                .shrink(topic, 5);

        ResizeController afterGrace = controller(
                Clock.fixed(START.plus(Duration.ofMinutes(6)), ZoneOffset.UTC),
                txs);

        afterGrace.completeTransition(topic);
        afterGrace.expand(topic, 10);

        txs.begin("T102");
        assertEquals(
                TransactionParticipantDecision.ADDED,
                txs.addParticipant("T102", topic, 7));
    }

    @Test
    void completedTransactionCannotAcceptNewParticipants() {
        TopicState topic = new TopicState("orders", 10);
        TransactionRegistry txs = new TransactionRegistry();

        txs.begin("T100");
        txs.commit("T100");

        assertEquals(
                TransactionParticipantDecision.REJECTED_TRANSACTION_NOT_OPEN,
                txs.addParticipant("T100", topic, 2));
    }
}
