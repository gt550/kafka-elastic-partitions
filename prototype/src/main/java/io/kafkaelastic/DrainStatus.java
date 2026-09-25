package io.kafkaelastic;

public enum DrainStatus {
    NOT_TRACKED,
    NO_COMMITTED_OFFSET,
    LAGGING,
    DRAINED
}
