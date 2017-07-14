package io.axway.iron.core;

import io.axway.iron.Command;
import io.axway.iron.StoreManagerFactory;
import io.axway.iron.core.internal.StoreManagerFactoryBuilderImpl;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStoreFactory;

public interface StoreManagerFactoryBuilder {
    static StoreManagerFactoryBuilder newStoreManagerBuilderFactory() {
        return new StoreManagerFactoryBuilderImpl();
    }

    StoreManagerFactoryBuilder withEntityClass(Class<?> entityClass);

    <T> StoreManagerFactoryBuilder withCommandClass(Class<? extends Command<T>> commandClass);

    StoreManagerFactoryBuilder withTransactionSerializer(TransactionSerializer transactionSerializer);

    StoreManagerFactoryBuilder withTransactionStoreFactory(TransactionStoreFactory transactionStoreFactory);

    StoreManagerFactoryBuilder withSnapshotSerializer(SnapshotSerializer snapshotSerializer);

    StoreManagerFactoryBuilder withSnapshotStoreFactory(SnapshotStoreFactory snapshotStoreFactory);

    StoreManagerFactory build();
}
