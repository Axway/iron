package io.axway.iron.spi.storage;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import org.reactivestreams.Publisher;

/**
 * SPI for snapshot store.
 */
public interface SnapshotStore {
    /**
     * Initiate the storage part of a snapshot for a given store.
     * In the end one snapshot will contain parts for every stores, so this method is called once for each store at snapshot time.
     *
     * @param storeName the name of the store about to be snapshot.
     * @param transactionId the transaction id of the snapshot to be written.
     * @return the {@code OutputStream} to be used to write the store's snapshot content.
     * @throws IOException in case of error when trying to provide access to the {@code OutputStream}
     */
    OutputStream createSnapshotWriter(String storeName, BigInteger transactionId) throws IOException;

    /**
     * Retrieve an existing snapshot in the store.
     *
     * @param transactionId the transaction id of the snapshot to be retrieved
     * @return the {@code Publisher<StoreSnapshotReader>} to be used to read the snapshot content. The publisher will provide one {@link StoreSnapshotReader} by store in the snapshot.
     * @throws IOException in case of error when trying to provide access to the one of the stores snapshots.
     */
    Publisher<StoreSnapshotReader> createSnapshotReader(BigInteger transactionId) throws IOException;

    /**
     * List all the existing snapshots.
     *
     * @return the list of snapshot transactions id.
     * @throws IOException if an I/O error occurs when listing the snapshots
     */
    List<BigInteger> listSnapshots() throws IOException;

    /**
     * Dispose any resource the store may have open.
     */
    default void close() {
        // default is to do nothing
    }

    /**
     * Delete a snapshot.
     *
     * @param transactionId the transaction id of the snapshot to be deleted.
     *  @throws IOException if an I/O error occurs when deleting the snapshot
     */
    void deleteSnapshot(BigInteger transactionId) throws IOException;

    /**
     * A reader to access the content of a snapshot for a store
     */
    interface StoreSnapshotReader {
        /**
         * @return the name of the store for which the snapshot can be read through {@link #inputStream()}
         */
        String storeName();

        /**
         * @return The InputStream to read the snapshot's part for the store
         * @throws IOException in case of an issue accessing to the snapshot
         */
        InputStream inputStream() throws IOException;
    }
}
