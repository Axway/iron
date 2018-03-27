package io.axway.iron.core;

import io.axway.iron.Command;
import io.axway.iron.StoreManagerFactory;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static io.axway.alf.assertion.Assertion.checkState;

public class FakeStoreManagerFactoryBuilderImpl implements StoreManagerFactoryBuilder {
    public TransactionSerializer m_transactionSerializer;
    public TransactionStoreFactory m_transactionStoreFactory;
    public SnapshotSerializer m_snapshotSerializer;
    public SnapshotStoreFactory m_snapshotStoreFactory;

    @Override
    public StoreManagerFactoryBuilder withEntityClass(Class<?> entityClass) {
        return null;  // not implement here
    }

    @Override
    public <T> StoreManagerFactoryBuilder withCommandClass(Class<? extends Command<T>> commandClass) {
        return null; // not implement here
    }

    @Override
    public StoreManagerFactoryBuilder withTransactionSerializer(TransactionSerializer transactionSerializer) {
        checkState(m_transactionSerializer == null, "Transaction serializer has been already set");
        m_transactionSerializer = transactionSerializer;
        return this;
    }

    @Override
    public StoreManagerFactoryBuilder withTransactionStoreFactory(TransactionStoreFactory transactionStoreFactory) {
        checkState(m_transactionStoreFactory == null, "Transaction store factory has been already set");
        m_transactionStoreFactory = transactionStoreFactory;
        return this;
    }

    @Override
    public StoreManagerFactoryBuilder withSnapshotSerializer(SnapshotSerializer snapshotSerializer) {
        checkState(m_snapshotSerializer == null, "Snapshot serializer has been already set");
        m_snapshotSerializer = snapshotSerializer;
        return this;
    }

    @Override
    public StoreManagerFactoryBuilder withSnapshotStoreFactory(SnapshotStoreFactory snapshotStoreFactory) {
        checkState(m_snapshotStoreFactory == null, "Snapshot store has been already set");
        m_snapshotStoreFactory = snapshotStoreFactory;
        return this;
    }

    @Override
    public StoreManagerFactory build() {
        return null; // not implement here
    }
}
