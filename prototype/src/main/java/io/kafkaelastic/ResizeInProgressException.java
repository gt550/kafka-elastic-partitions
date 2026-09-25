package io.kafkaelastic;

public final class ResizeInProgressException extends ResizeException {

    public ResizeInProgressException(String message) {
        super(message);
    }
}
