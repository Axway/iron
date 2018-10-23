package io.axway.iron.spi.kafka;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.reactivestreams.Publisher;
import io.axway.iron.spi.StoreNamePrefixManagement;
import io.axway.iron.spi.storage.TransactionStore;
import io.reactivex.Emitter;
import io.reactivex.Flowable;
import io.reactivex.functions.BiConsumer;
import io.reactivex.schedulers.Schedulers;

import static io.axway.iron.spi.StoreNamePrefixManagement.readStoreName;
import static java.lang.Long.parseLong;
import static java.time.Duration.ofMillis;
import static java.util.Collections.*;
import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class KafkaTransactionStore implements TransactionStore {
    private static final int PARTITION = 0;
    private static final int CONSTANT_KEY = 0;
    private static final int RETRIES = 5;
    private static final int PRODUCER_BUFFER_MEMORY = 33554432;
    private static final String CONSUMER_SESSION_TIMEOUT = "30000";
    private static final long NO_SEEK = -1;

    private final String m_topicName;
    private final TopicPartition m_topicPartition;
    private final Producer<Integer, byte[]> m_producer;

    private final AtomicLong m_pendingSeek = new AtomicLong(NO_SEEK);
    private final Flowable<TransactionInput> m_transactionsFlow;
    private final StoreNamePrefixManagement m_prefixManagement = new StoreNamePrefixManagement();

    KafkaTransactionStore(Properties kafkaProperties, String topicName) {
        m_topicName = topicName;
        m_topicPartition = new TopicPartition(m_topicName, PARTITION);
        UUID uuid = UUID.randomUUID();

        //we create the topic first as a workaround of bug https://issues.apache.org/jira/browse/KAFKA-3727
        createKafkaTopic((Properties) kafkaProperties.clone(), topicName);

        // see https://stackoverflow.com/questions/37363119/kafka-producer-org-apache-kafka-common-serialization-stringserializer-could-no
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(null);
            Properties producerKafkaProperties = (Properties) kafkaProperties.clone();
            producerKafkaProperties.put(ACKS_CONFIG, "all");
            producerKafkaProperties.put(RETRIES_CONFIG, RETRIES);
            producerKafkaProperties.put(BATCH_SIZE_CONFIG, 1);
            producerKafkaProperties.put(BUFFER_MEMORY_CONFIG, PRODUCER_BUFFER_MEMORY);
            producerKafkaProperties.put(CLIENT_ID_CONFIG, "ironClient-" + uuid);
            m_producer = new KafkaProducer<>(producerKafkaProperties, new IntegerSerializer(), new ByteArraySerializer());
        } finally {
            Thread.currentThread().setContextClassLoader(classLoader);
        }

        m_transactionsFlow = Flowable      //
                .generate(() -> {
                    // see https://stackoverflow.com/questions/37363119/kafka-producer-org-apache-kafka-common-serialization-stringserializer-could-no
                              ClassLoader ccl = Thread.currentThread().getContextClassLoader();
                              KafkaConsumer<Integer, byte[]> kafkaConsumer;
                              try {
                                  Thread.currentThread().setContextClassLoader(null);
                                  Properties consumerKafkaProperties = (Properties) kafkaProperties.clone();
                                  consumerKafkaProperties.put(MAX_POLL_RECORDS_CONFIG, 1);
                                  consumerKafkaProperties.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
                                  consumerKafkaProperties.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
                                  consumerKafkaProperties.put(SESSION_TIMEOUT_MS_CONFIG, CONSUMER_SESSION_TIMEOUT);
                                  consumerKafkaProperties.put(GROUP_ID_CONFIG, "ironGroup-" + uuid);
                                  kafkaConsumer = new KafkaConsumer<>(consumerKafkaProperties, new IntegerDeserializer(),
                                                                      new ByteArrayDeserializer());
                              } finally {
                                  Thread.currentThread().setContextClassLoader(ccl);
                              }
                              kafkaConsumer.assign(singletonList(m_topicPartition));
                              return kafkaConsumer;
                          },  //
                          (BiConsumer<KafkaConsumer<Integer, byte[]>, Emitter<ConsumerRecords<Integer, byte[]>>>) (consumer, emitter) -> {
                              long seek = m_pendingSeek.getAndSet(NO_SEEK);
                              if (seek > NO_SEEK) {
                                  consumer.seek(m_topicPartition, seek);
                              }

                              emitter.onNext(consumer.poll(ofMillis(parseLong(CONSUMER_SESSION_TIMEOUT) / 5)));
                          },                                                                          //
                          KafkaConsumer::close)  //
                .subscribeOn(Schedulers.io())                                               // event loop
                .observeOn(Schedulers.computation())                                        //
                .concatMap(Flowable::fromIterable)                                          //
                .map(this::prepareTransactionInput);
    }

    @Override
    public OutputStream createTransactionOutput(String storeName) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();
                m_producer.send(new ProducerRecord<>(m_topicName, PARTITION, CONSTANT_KEY, toByteArray()));
            }
        };

        m_prefixManagement.writeNamePrefix(storeName, outputStream);
        return outputStream;
    }

    @Override
    public Publisher<TransactionInput> allTransactions() {
        return m_transactionsFlow;
    }

    @Override
    public void seekTransaction(BigInteger latestProcessedTransactionId) {
        m_pendingSeek.set(latestProcessedTransactionId.longValueExact() + 1);
    }

    @Override
    public void close() {
        m_producer.flush();
        m_producer.close();
    }

    private void createKafkaTopic(Properties kafkaProperties, String topicName) {
        try (AdminClient adminClient = AdminClient.create(kafkaProperties)) {
            adminClient.createTopics(singleton(new NewTopic(topicName, 1, (short) 1)))//
                    .all().get();
        } catch (Exception e) {
            Throwable current = e;
            while(!(current instanceof TopicExistsException) && current.getCause() != null) {
                 current = current.getCause();
            }
            if(!(current instanceof TopicExistsException)) throw new RuntimeException(e);
        }
    }

    private TransactionInput prepareTransactionInput(ConsumerRecord<Integer, byte[]> record) throws IOException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(record.value());
        String storeName = readStoreName(inputStream);

        return new TransactionInput() {
            @Override
            public InputStream getInputStream() {
                return inputStream;
            }

            @Override
            public BigInteger getTransactionId() {
                return BigInteger.valueOf(record.offset());
            }

            @Override
            public String storeName() {
                return storeName;
            }
        };
    }
}
