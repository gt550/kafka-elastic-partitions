package io.kafkaelastic;

import java.time.Clock;
import java.time.Instant;

public final class BrokerWriteGate {

    private final Clock clock;

    public BrokerWriteGate(Clock clock) {
        this.clock = clock;
    }

    public ProduceDecision evaluate(TopicState topic, int partitionId) {
        final PartitionState partition;

        try {
            partition = topic.partition(partitionId);
        } catch (IllegalArgumentException e) {
            return ProduceDecision.REJECTED_UNKNOWN_PARTITION;
        }

        return switch (partition.lifecycleState()) {
            case ACTIVE -> ProduceDecision.ACCEPTED;

            case TRANSITIONING -> {
                Instant deadline = partition.transitionDeadline();

                if (deadline != null && clock.instant().isBefore(deadline)) {
                    yield ProduceDecision.ACCEPTED;
                }

                yield ProduceDecision.REJECTED_NOT_WRITABLE;
            }

            case RETIRED -> ProduceDecision.REJECTED_NOT_WRITABLE;
        };
    }
}
