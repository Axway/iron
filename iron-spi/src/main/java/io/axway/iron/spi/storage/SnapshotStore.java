package io.axway.iron.spi.storage;

import java.io.*;
import java.math.BigInteger;
import java.util.*;

/**
 * Snapshot storage interface.
 */
public interface SnapshotStore {
    /**
     * Initiate the storage of a new snapshot.
     *
     * @param transactionId the transaction id of the snapshot to be written.
     * @return the {@code OutputStream} to be used to write the snapshot content.
     * @throws IOException in case of error when trying to provide access to the {@code OutputStream}
     */
    OutputStream createSnapshotWriter(BigInteger transactionId) throws IOException;

    /**
     * Retrieve an existing snapshot in the store.
     *
     * @param transactionId the transaction id of the snapshot to be retrieved
     * @return the {@code InputStream} to be used to read the snapshot content.
     * @throws IOException in case of error when trying to provide access to the {@code InputStream}
     */
    InputStream createSnapshotReader(BigInteger transactionId) throws IOException;

    /**
     * List all the existing snapshot.
     *
     * @return the list of snapshot transactions id.
     */
    List<BigInteger> listSnapshots();

    /**
     * Delete a snapshot.
     *
     * @param transactionId the transaction id of the snapshot to be deleted.
     */
    void deleteSnapshot(BigInteger transactionId);
}
