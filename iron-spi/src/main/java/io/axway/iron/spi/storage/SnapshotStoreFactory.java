package io.axway.iron.spi.storage;

/**
 * SPI for snapshot store.
 */
public interface SnapshotStoreFactory {
    /**
     * Create a snapshot store for the given store.
     *
     * @param storeName the name of the store
     * @return the {@link SnapshotStore} to be used to access to the snapshot for the given store
     */
    SnapshotStore createSnapshotStore(String storeName);
}
