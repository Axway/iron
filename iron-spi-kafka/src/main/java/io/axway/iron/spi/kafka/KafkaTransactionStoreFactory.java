package io.axway.iron.spi.kafka;

import java.util.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import io.axway.iron.spi.storage.TransactionStore;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static io.axway.alf.assertion.Assertion.checkNotNullNorEmpty;

public class KafkaTransactionStoreFactory implements TransactionStoreFactory {
    private final Properties m_kafkaProperties;

    KafkaTransactionStoreFactory(Properties kafkaProperties) {
        m_kafkaProperties = kafkaProperties;
    }

    @Override
    public TransactionStore createTransactionStore(String storeName) {
        String topicName = storeName.trim();
        checkNotNullNorEmpty(topicName, "Topic name can't be null or empty");

        //we create the topic first as a workaround of bug https://issues.apache.org/jira/browse/KAFKA-3727
        createKafkaTopic(m_kafkaProperties, topicName);

        return new KafkaTransactionStore(m_kafkaProperties, topicName);
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
