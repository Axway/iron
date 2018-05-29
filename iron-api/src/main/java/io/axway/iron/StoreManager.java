package io.axway.iron;

import java.math.BigInteger;
import java.util.*;
import java.util.regex.*;
import javax.annotation.*;

/**
 * A store manager permits to open many stores sharing the same model definition but not the same model instances.
 */
public interface StoreManager extends AutoCloseable {
    /**
     * Any store name used in the {@link #getStore(String)} method must comply with this pattern.
     */
    Pattern STORE_NAME_VALIDATOR_PATTERN = Pattern.compile("[\\w-]+");

    /**
     * Lists every stores already existing in this StoreManager
     */
    Set<String> listStores();

    /**
     * Retrieves a store. If the store already exists it's recovered from the latest snapshot and from the command redolog. Else a new store is created.
     *
     * @param storeName the name of the store
     * @return the {@link Store}
     */
    Store getStore(String storeName);

    /**
     * Create a new snapshot of all stores created by this manager.
     * <p>
     * A recent snapshot permits to speedup store recovery.
     * <p>
     * No new snapshot is created if no change was made in the stores since the last snapshot was created.
     *
     * @return the transaction ID of the snapshot or {@code null} if no new snapshot was created
     */
    @Nullable
    BigInteger snapshot();

    /**
     * Close the stores, mainly stop the redolog processing thread.
     */
    void close();

    /**
     * The transaction ID of the latest snapshot.
     *
     * @return the transaction ID of the last created snapshot
     */
    BigInteger lastSnapshotTransactionId();
}
