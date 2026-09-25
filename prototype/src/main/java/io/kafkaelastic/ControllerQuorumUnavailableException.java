package io.kafkaelastic;

public final class ControllerQuorumUnavailableException extends ResizeException {

    public ControllerQuorumUnavailableException(String message) {
        super(message);
    }
}
