package io.kafkaelastic;

/**
 * Minimal stand-in for KRaft metadata-quorum availability.
 *
 * Lifecycle changes require an available quorum. Time passing by itself never
 * advances a partition from TRANSITIONING to RETIRED or RETIRED to deleted.
 */
public final class MetadataQuorum {

    private boolean available;

    public MetadataQuorum(boolean available) {
        this.available = available;
    }

    public boolean isAvailable() {
        return available;
    }

    public void setAvailable(boolean available) {
        this.available = available;
    }
}
