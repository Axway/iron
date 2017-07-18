package io.axway.iron.spi.consul;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.SnapshotStoreFactory;

public class ConsulSnapshotStoreFactory implements SnapshotStoreFactory {
    private final ObjectMapper m_objectMapper;
    private final String m_consulAddress;

    public ConsulSnapshotStoreFactory(String consulAddress) {
        m_objectMapper = new ObjectMapper();
        m_consulAddress = consulAddress;
    }

    @Override
    public SnapshotStore createSnapshotStore(String storeName) {
        return new ConsulSnapshotStore(m_objectMapper, m_consulAddress, storeName);
    }
}
