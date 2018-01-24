package io.axway.iron.spi.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import io.axway.iron.spi.storage.TransactionStore;
import io.axway.iron.spi.storage.TransactionStoreFactory;

/**
 * Kinesis transaction store factory to build Kinesis TransactionStore.
 */
public class KinesisTransactionStoreFactory implements TransactionStoreFactory {

    private KinesisProducer m_producer;
    private AmazonKinesis m_consumer;

    /**
     * This constructor allows to set region, kinesis endpoint and cloudwatch endpoint. Only useful for local testing.
     */
    public KinesisTransactionStoreFactory(KinesisProducer producer, AmazonKinesis consumer) {
        m_producer = producer;
        m_consumer = consumer;
    }

    @Override
    public TransactionStore createTransactionStore(String storeName) {
        return new KinesisTransactionStore(m_producer, m_consumer, storeName);
    }
}
