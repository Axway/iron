package io.axway.iron.spi.kafka;

import java.util.*;
import java.util.function.*;
import io.axway.iron.spi.storage.TransactionStore;

public class KafkaTransactionStoreBuilder implements Supplier<TransactionStore> {
    private Properties m_properties;
    private String m_name;

    public KafkaTransactionStoreBuilder(String name) {
        m_name = name;
    }

    public KafkaTransactionStoreBuilder setProperties(Properties properties) {
        m_properties = properties;
        return this;
    }

    @Override
    public TransactionStore get() {
        return new KafkaTransactionStore(m_properties, m_name);
    }
}
