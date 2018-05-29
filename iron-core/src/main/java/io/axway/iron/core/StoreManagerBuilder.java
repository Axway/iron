package io.axway.iron.core;

import java.util.*;
import io.axway.iron.Command;
import io.axway.iron.StoreManager;
import io.axway.iron.core.internal.StoreManagerBuilderImpl;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

public interface StoreManagerBuilder {
    static StoreManagerBuilder newStoreManagerBuilder() {
        return new StoreManagerBuilderImpl();
    }

    static StoreManagerBuilder newStoreManagerBuilder(String name, Properties properties) {
        return new StoreManagerBuilderImpl(name, properties);
    }

    StoreManagerBuilder withEntityClass(Class<?> entityClass);

    <T> StoreManagerBuilder withCommandClass(Class<? extends Command<T>> commandClass);

    StoreManagerBuilder withTransactionSerializer(TransactionSerializer transactionSerializer);

    StoreManagerBuilder withTransactionStore(TransactionStore transactionStore);

    StoreManagerBuilder withSnapshotSerializer(SnapshotSerializer snapshotSerializer);

    StoreManagerBuilder withSnapshotStore(SnapshotStore snapshotStore);

    StoreManager build();
}
