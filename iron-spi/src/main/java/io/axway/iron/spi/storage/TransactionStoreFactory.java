package io.axway.iron.spi.storage;

/**
 * SPI for transaction store.
 */
public interface TransactionStoreFactory {
    /**
     * Create a transaction store (eg redolog) for the given store.
     *
     * @param storeName the name of the store
     * @return the {@link TransactionStore} to be used to access to the store redolog
     */
    TransactionStore createTransactionStore(String storeName);
}
