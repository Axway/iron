package io.axway.iron.spi.model.snapshot;

import java.math.BigInteger;
import java.util.*;
import javax.xml.bind.annotation.*;

@XmlType(propOrder = {"snapshotModelVersion", "transactionId", "entities"})
public class SerializableSnapshot {
    public static final long SNAPSHOT_MODEL_VERSION = 1;

    private long m_snapshotModelVersion;
    private BigInteger m_transactionId;
    private Collection<SerializableEntity> m_entities;

    public long getSnapshotModelVersion() {
        return m_snapshotModelVersion;
    }

    public void setSnapshotModelVersion(long snapshotModelVersion) {
        m_snapshotModelVersion = snapshotModelVersion;
    }

    public BigInteger getTransactionId() {
        return m_transactionId;
    }

    public void setTransactionId(BigInteger transactionId) {
        m_transactionId = transactionId;
    }

    public Collection<SerializableEntity> getEntities() {
        return m_entities;
    }

    public void setEntities(Collection<SerializableEntity> entities) {
        m_entities = entities;
    }
}
