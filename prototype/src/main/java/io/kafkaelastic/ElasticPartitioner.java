package io.kafkaelastic;

import java.util.List;
import java.util.Objects;

public final class ElasticPartitioner {

    public int selectPartition(Object key, ProducerMetadata metadata) {
        Objects.requireNonNull(metadata, "metadata");

        List<ProducerMetadata.PartitionView> writable =
                metadata.writablePartitions();

        if (writable.isEmpty()) {
            throw new ResizeException(
                    "No ACTIVE partitions available for topic "
                            + metadata.topicName());
        }

        int hash = key == null ? 0 : key.hashCode();
        int index = Math.floorMod(hash, writable.size());

        return writable.get(index).partitionId();
    }
}
