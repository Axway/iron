package io.axway.iron;

import java.math.BigInteger;
import javax.annotation.*;

/**
 * Permits to manage the store sanity and lifecycle.
 */
public interface StoreManager extends AutoCloseable {
    /**
     * @return the {@link Store} instance that can be used by business methods to interact with the store data
     */
    Store getStore();

    /**
     * Create a new snapshot.
     * <p>
     * A recent snapshot permits to speedup store recovery.
     * <p>
     * No new snapshot is created if no change was made in the store since the last snapshot was created.
     *
     * @return the transaction ID of the snapshot or {@code null} if no new snapshot was created
     */
    @Nullable
    BigInteger snapshot();

    /**
     * Close the store, mainly stop the redolog processing thread.
     */
    void close();

    /**
     * Return the transaction ID of the last created snapshot.
     *
     * @return the transaction ID of the last created snapshot
     */
    BigInteger lastSnapshotTransactionId();
}
