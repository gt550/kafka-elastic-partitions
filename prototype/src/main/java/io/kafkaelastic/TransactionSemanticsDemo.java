package io.kafkaelastic;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

public final class TransactionSemanticsDemo {

    public static void main(String[] args) {
        Instant start = Instant.parse("2026-09-25T15:00:00Z");
        TopicState topic = new TopicState("orders", 10);
        TransactionRegistry transactions = new TransactionRegistry();

        transactions.begin("T100");
        System.out.println("T100 add P7 before resize: "
                + transactions.addParticipant("T100", topic, 7));

        ResizeController first = new ResizeController(
                Clock.fixed(start, ZoneOffset.UTC),
                Duration.ofMinutes(5),
                Duration.ofDays(30),
                transactions);

        first.shrink(topic, 5);

        transactions.begin("T101");
        System.out.println("T101 add P7 after transition starts: "
                + transactions.addParticipant("T101", topic, 7));

        System.out.println("T100 reuse P7 after transition starts: "
                + transactions.addParticipant("T100", topic, 7));

        ResizeController afterGrace = new ResizeController(
                Clock.fixed(start.plus(Duration.ofMinutes(6)), ZoneOffset.UTC),
                Duration.ofMinutes(5),
                Duration.ofDays(30),
                transactions);

        try {
            afterGrace.completeTransition(topic);
        } catch (ResizeException e) {
            System.out.println("Retirement blocked: " + e.getMessage());
        }

        transactions.commit("T100");
        System.out.println("T100 committed");

        afterGrace.completeTransition(topic);
        System.out.println("P7 lifecycle: "
                + topic.partition(7).lifecycleState());

        afterGrace.expand(topic, 10);

        transactions.begin("T102");
        System.out.println("T102 add reactivated P7: "
                + transactions.addParticipant("T102", topic, 7));
    }
}
