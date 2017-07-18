package io.axway.iron.spi.kafka;

import java.util.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import io.axway.iron.spi.storage.TransactionStore;
import io.axway.iron.spi.storage.TransactionStoreFactory;

public class KafkaTransactionStoreFactory implements TransactionStoreFactory {
    private final Properties m_kafkaProperties;

    public KafkaTransactionStoreFactory(Properties kafkaProperties) {
        m_kafkaProperties = kafkaProperties;
    }

    @Override
    public TransactionStore createTransactionStore(String storeName) {
        //we create the topic first as a workaround of bug https://issues.apache.org/jira/browse/KAFKA-3727
        createKafkaTopic(m_kafkaProperties, storeName);

        return new KafkaTransactionStore(m_kafkaProperties, storeName);
    }

    private void createKafkaTopic(Properties kafkaProperties, String topicName) {
        Properties localKafkaProperties = (Properties) kafkaProperties.clone();
        localKafkaProperties.put("group.id", "bug-" + UUID.randomUUID());
        localKafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        localKafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        KafkaConsumer consumer = new KafkaConsumer<>(localKafkaProperties);

        // if topic auto create is on then subscription creates the topic
        consumer.subscribe(Collections.singletonList(topicName));
        consumer.poll(100);
        consumer.close();
    }
}
