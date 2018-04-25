package io.axway.iron.spi.kafka;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import io.axway.iron.spi.storage.TransactionStore;

import static io.axway.alf.assertion.Assertion.checkState;
import static java.util.Collections.*;
import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

class KafkaTransactionStore implements TransactionStore {
    private static final int PARTITION = 0;
    private static final int CONSTANT_KEY = 0;
    private static final int RETRIES = 5;
    private static final int PRODUCER_BUFFER_MEMORY = 33554432;
    private static final String CONSUMER_SESSION_TIMEOUT = "30000";

    private final String m_topicName;
    private final TopicPartition m_topicPartition;
    private final Consumer<Integer, byte[]> m_consumer;
    private final Producer<Integer, byte[]> m_producer;

    KafkaTransactionStore(Properties kafkaProperties, String topicName) {
        m_topicName = topicName;
        m_topicPartition = new TopicPartition(m_topicName, PARTITION);

        UUID uuid = UUID.randomUUID();

        Properties producerKafkaProperties = (Properties) kafkaProperties.clone();
        producerKafkaProperties.put(ACKS_CONFIG, "all");
        producerKafkaProperties.put(RETRIES_CONFIG, RETRIES);
        producerKafkaProperties.put(BATCH_SIZE_CONFIG, 1);
        producerKafkaProperties.put(BUFFER_MEMORY_CONFIG, PRODUCER_BUFFER_MEMORY);
        producerKafkaProperties.put(CLIENT_ID_CONFIG, "ironClient-" + uuid);
        m_producer = new KafkaProducer<>(producerKafkaProperties, new IntegerSerializer(), new ByteArraySerializer());

        Properties consumerKafkaProperties = (Properties) kafkaProperties.clone();
        consumerKafkaProperties.put(MAX_POLL_RECORDS_CONFIG, 1);
        consumerKafkaProperties.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerKafkaProperties.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerKafkaProperties.put(SESSION_TIMEOUT_MS_CONFIG, CONSUMER_SESSION_TIMEOUT);
        consumerKafkaProperties.put(GROUP_ID_CONFIG, "ironGroup-" + uuid);
        m_consumer = new KafkaConsumer<>(consumerKafkaProperties, new IntegerDeserializer(), new ByteArrayDeserializer());
        m_consumer.assign(singletonList(m_topicPartition));
    }

    @Override
    public OutputStream createTransactionOutput() {
        return new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();
                m_producer.send(new ProducerRecord<>(m_topicName, PARTITION, CONSTANT_KEY, toByteArray()));
            }
        };
    }

    @Override
    public void seekTransactionPoll(BigInteger latestProcessedTransactionId) {
        m_consumer.seek(m_topicPartition, latestProcessedTransactionId.longValueExact() + 1);
    }

    @Override
    public TransactionInput pollNextTransaction(long timeout, TimeUnit unit) {
        Iterator<ConsumerRecord<Integer, byte[]>> iterator = m_consumer.poll(unit.toMillis(timeout)).iterator();
        if (!iterator.hasNext()) {
            return null;
        }

        ConsumerRecord<Integer, byte[]> firstRecord = iterator.next();
        checkState(!iterator.hasNext(), "Kafka should not return more than one record");

        return new TransactionInput() {
            @Override
            public InputStream getInputStream() {
                return new ByteArrayInputStream(firstRecord.value());
            }

            @Override
            public BigInteger getTransactionId() {
                return BigInteger.valueOf(firstRecord.offset());
            }
        };
    }
}
