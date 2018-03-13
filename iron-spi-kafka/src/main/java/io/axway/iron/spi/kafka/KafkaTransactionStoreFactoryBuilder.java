package io.axway.iron.spi.kafka;

import java.util.*;
import java.util.function.*;
import io.axway.iron.spi.storage.TransactionStoreFactory;

public class KafkaTransactionStoreFactoryBuilder implements Supplier<TransactionStoreFactory> {
    private Properties m_properties;

    public KafkaTransactionStoreFactoryBuilder setProperties(Properties properties) {
        m_properties = properties;
        return this;
    }

    @Override
    public TransactionStoreFactory get() {
        return new KafkaTransactionStoreFactory(m_properties);
    }
}