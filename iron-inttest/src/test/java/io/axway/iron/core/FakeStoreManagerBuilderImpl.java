package io.axway.iron.core;

import io.axway.iron.Command;
import io.axway.iron.StoreManager;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

import static io.axway.alf.assertion.Assertion.checkState;

public class FakeStoreManagerBuilderImpl implements StoreManagerBuilder {
    public TransactionSerializer m_transactionSerializer;
    public TransactionStore m_transactionStore;
    public SnapshotSerializer m_snapshotSerializer;
    public SnapshotStore m_snapshotStore;

    @Override
    public StoreManagerBuilder withEntityClass(Class<?> entityClass) {
        return null;  // not implement here
    }

    @Override
    public <T> StoreManagerBuilder withCommandClass(Class<? extends Command<T>> commandClass) {
        return null; // not implement here
    }

    @Override
    public StoreManagerBuilder withTransactionSerializer(TransactionSerializer transactionSerializer) {
        checkState(m_transactionSerializer == null, "Transaction serializer has been already set");
        m_transactionSerializer = transactionSerializer;
        return this;
    }

    @Override
    public StoreManagerBuilder withTransactionStore(TransactionStore transactionStore) {
        checkState(m_transactionStore == null, "Transaction store factory has been already set");
        m_transactionStore = transactionStore;
        return this;
    }

    @Override
    public StoreManagerBuilder withSnapshotSerializer(SnapshotSerializer snapshotSerializer) {
        checkState(m_snapshotSerializer == null, "Snapshot serializer has been already set");
        m_snapshotSerializer = snapshotSerializer;
        return this;
    }

    @Override
    public StoreManagerBuilder withSnapshotStore(SnapshotStore snapshotStore) {
        checkState(m_snapshotStore == null, "Snapshot store has been already set");
        m_snapshotStore = snapshotStore;
        return this;
    }

    @Override
    public StoreManager build() {
        return null; // not implement here
    }
}
