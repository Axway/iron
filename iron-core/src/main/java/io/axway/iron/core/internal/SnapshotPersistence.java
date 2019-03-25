package io.axway.iron.core.internal;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import io.axway.iron.core.internal.entity.EntityStore;
import io.axway.iron.error.StoreException;
import io.axway.iron.spi.model.snapshot.SerializableSnapshot;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.storage.SnapshotStore;

import static io.axway.iron.spi.model.snapshot.SerializableSnapshot.SNAPSHOT_MODEL_VERSION;
import static java.util.stream.Collectors.*;

public class SnapshotPersistence {
    private final SnapshotStore.SnapshotStoreWriter m_snapshotWriter;
    private SnapshotSerializer m_snapshotSerializer;
    private BigInteger m_transactionId;
    private long m_applicationModelVersion;

    public SnapshotPersistence(long applicationModelVersion, SnapshotStore snapshotStore, SnapshotSerializer snapshotSerializer, BigInteger transactionId) {
        m_applicationModelVersion = applicationModelVersion;
        m_snapshotSerializer = snapshotSerializer;
        m_transactionId = transactionId;
        m_snapshotWriter = snapshotStore.createSnapshotWriter(m_transactionId);
    }

    public void persist(String storeName, List<EntityStore<?>> entityStores) {
        SerializableSnapshot serializableSnapshot = new SerializableSnapshot();
        serializableSnapshot.setSnapshotModelVersion(SNAPSHOT_MODEL_VERSION);
        serializableSnapshot.setApplicationModelVersion(m_applicationModelVersion);
        serializableSnapshot.setTransactionId(m_transactionId);
        serializableSnapshot.setEntities(entityStores.stream().map(EntityStore::snapshot).collect(toList()));

        try (OutputStream out = m_snapshotWriter.getOutputStream(storeName)) {
            m_snapshotSerializer.serializeSnapshot(out, serializableSnapshot);
        } catch (IOException e) {
            throw new StoreException("Error when creating the store snapshot", args -> args.add("transactionId", m_transactionId), e);
        }
    }

    public void commit() {
        m_snapshotWriter.commit();
    }
}
