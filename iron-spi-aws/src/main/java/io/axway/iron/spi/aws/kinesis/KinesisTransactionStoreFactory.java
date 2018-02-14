package io.axway.iron.spi.aws.kinesis;

import java.util.*;
import com.amazonaws.services.kinesis.AmazonKinesis;
import io.axway.iron.spi.storage.TransactionStore;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static io.axway.iron.spi.aws.kinesis.AwsKinesisUtils.buildKinesisClient;

/**
 * Kinesis transaction store factory to build Kinesis TransactionStore.
 */
public class KinesisTransactionStoreFactory implements TransactionStoreFactory {

    private AmazonKinesis m_kinesisClient;

    /**
     * Create a KinesisTransactionStoreFactory with some properties set:
     *
     * @param properties
     */
    public KinesisTransactionStoreFactory(Properties properties) {
        m_kinesisClient = buildKinesisClient(properties);
    }

    @Override
    public TransactionStore createTransactionStore(String storeName) {
        return new KinesisTransactionStore(m_kinesisClient, storeName);
    }
}
