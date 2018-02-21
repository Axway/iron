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
import io.axway.iron.spi.storage.TransactionStore;

/**
 * TODO check if commit is needed or not<br>
 * TODO check why sometimes polling returns nothing (for a long time) when it shouldn't<br>
 * TODO write tests with embedded kafka<br>
 * TODO add Logging<br>
 */
class KafkaTransactionStore implements TransactionStore {
    private static final int PARTITION = 0;
    private static final int CONSTANT_KEY = 0;
    private static final int RETRIES = 5;
    private static final int PRODUCER_BUFFER_MEMORY = 33554432;
    private static final String CONSUMER_SESSION_TIMEOUT = "30000";

    private final Consumer<Integer, byte[]> m_consumer;
    private final Producer<Integer, byte[]> m_producer;
    private final String m_topicName;
    private final TopicPartition m_topicPartition;

    KafkaTransactionStore(Properties kafkaProperties, String topicName) {
        m_topicName = topicName.trim();
        if (m_topicName.isEmpty()) {
            throw new IllegalArgumentException("Topic name can't be null");
        }
        m_topicPartition = new TopicPartition(m_topicName, PARTITION);

        UUID uuid = UUID.randomUUID();

        Properties producerKafkaProperties = (Properties) kafkaProperties.clone();
        producerKafkaProperties.put("acks", "all");
        producerKafkaProperties.put("retries", RETRIES);
        producerKafkaProperties.put("batch.size", 1);
        producerKafkaProperties.put("linger.ms", 1);
        producerKafkaProperties.put("buffer.memory", PRODUCER_BUFFER_MEMORY);
        producerKafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        producerKafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerKafkaProperties.put("client.id", "ironClient-" + uuid);
        m_producer = new KafkaProducer<>(producerKafkaProperties);

        Properties consumerKafkaProperties = (Properties) kafkaProperties.clone();
        consumerKafkaProperties.put("max.poll.records", 1);
        consumerKafkaProperties.put("auto.offset.reset", "earliest");
        consumerKafkaProperties.put("enable.auto.commit", "false");
        consumerKafkaProperties.put("session.timeout.ms", CONSUMER_SESSION_TIMEOUT);
        consumerKafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        consumerKafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerKafkaProperties.put("group.id", "ironGroup-" + uuid);

        m_consumer = new KafkaConsumer<>(consumerKafkaProperties);
        m_consumer.subscribe(Collections.singletonList(m_topicName));
    }

    @Override
    public OutputStream createTransactionOutput() throws IOException {
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
        if (iterator.hasNext()) {
            throw new IllegalStateException("Kafka should not return more than one record");
        }

        return new TransactionInput() {
            @Override
            public InputStream getInputStream() throws IOException {
                return new ByteArrayInputStream(firstRecord.value());
            }

            @Override
            public BigInteger getTransactionId() {
                return BigInteger.valueOf(firstRecord.offset());
            }
        };
    }
}
