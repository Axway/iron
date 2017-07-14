package io.axway.iron.core.spi.testing;

import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStore;
import io.axway.iron.spi.storage.TransactionStoreFactory;

public class TransientStoreFactory implements SnapshotStoreFactory, TransactionStoreFactory {
    @Override
    public SnapshotStore createSnapshotStore(String storeName) {
        return new TransientSnapshotStore();
    }

    @Override
    public TransactionStore createTransactionStore(String storeName) {
        return new TransientTransactionStore();
    }
}
