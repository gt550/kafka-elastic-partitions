package io.kafkaelastic;

public final class StaleResizeEpochException extends ResizeException {

    public StaleResizeEpochException(String message) {
        super(message);
    }
}
