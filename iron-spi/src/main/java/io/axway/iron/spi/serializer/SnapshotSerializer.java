package io.axway.iron.spi.serializer;

import java.io.*;
import io.axway.iron.spi.model.snapshot.SerializableSnapshot;

/**
 * Snapshot serialization interface.<br>
 * It reads/writes model entities instances data.
 */
public interface SnapshotSerializer {
    /**
     * Serialize a snapshot.
     *
     * @param out the stream where the serialized snapshot must be written
     * @param serializableSnapshot the snapshot to be serialized
     * @throws IOException in case of errors when writing on the {@code out} stream
     */
    void serializeSnapshot(OutputStream out, SerializableSnapshot serializableSnapshot) throws IOException;

    /**
     * Deserialize a snapshot.
     *
     *
     * @param storeName the name of the store
     * @param in the snapshot is to be read from this stream
     * @return the deserialized snapshot
     * @throws IOException in case of errors when writing on the {@code in} stream
     */
    SerializableSnapshot deserializeSnapshot(String storeName, InputStream in) throws IOException;
}
