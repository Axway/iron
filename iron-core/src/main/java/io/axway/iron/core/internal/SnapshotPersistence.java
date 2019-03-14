package io.axway.iron.core.internal;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.function.*;
import io.axway.iron.core.internal.entity.EntityStore;
import io.axway.iron.error.StoreException;
import io.axway.iron.spi.model.snapshot.SerializableSnapshot;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.storage.SnapshotStore;

import static io.axway.iron.spi.model.snapshot.SerializableSnapshot.SNAPSHOT_MODEL_VERSION;
import static java.util.stream.Collectors.*;

public class SnapshotPersistence {
    private final Function<String, OutputStream> m_storeToOutputStream;
    private final Supplier<Void> m_onSuccess;
    private SnapshotSerializer m_snapshotSerializer;
    private BigInteger m_transactionId;

    public SnapshotPersistence(SnapshotStore snapshotStore, SnapshotSerializer snapshotSerializer, BigInteger transactionId) {
        m_snapshotSerializer = snapshotSerializer;
        m_transactionId = transactionId;
        SnapshotStore.SnapshotStoreWriter snapshotWriter = snapshotStore.createSnapshotWriter(m_transactionId);
        m_storeToOutputStream = snapshotWriter.storeToOutputStream();
        m_onSuccess = snapshotWriter.onSuccess();
    }

    public void persist(String storeName, List<EntityStore<?>> entityStores) {
        SerializableSnapshot serializableSnapshot = new SerializableSnapshot();
        serializableSnapshot.setSnapshotModelVersion(SNAPSHOT_MODEL_VERSION);
        serializableSnapshot.setTransactionId(m_transactionId);
        serializableSnapshot.setEntities(entityStores.stream().map(EntityStore::snapshot).collect(toList()));

        try (OutputStream out = m_storeToOutputStream.apply(storeName)) {
            m_snapshotSerializer.serializeSnapshot(out, serializableSnapshot);
        } catch (IOException e) {
            throw new StoreException("Error when creating the store snapshot", args -> args.add("transactionId", m_transactionId), e);
        }
    }

    // FIXME I disagreed to use Closeable, because close() should called whatever happens in order to release resources
    // FIXME this method should be called only in case of success
    public void onSuccess() {
        m_onSuccess.get();
    }
}
