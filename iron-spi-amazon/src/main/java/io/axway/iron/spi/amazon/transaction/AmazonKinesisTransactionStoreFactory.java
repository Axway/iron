package io.axway.iron.spi.amazon.transaction;

import io.axway.iron.spi.storage.TransactionStore;
import io.axway.iron.spi.storage.TransactionStoreFactory;

public class AmazonKinesisTransactionStoreFactory implements TransactionStoreFactory {
    @Override
    public TransactionStore createTransactionStore(String storeName) {
        return null;
    }
}
