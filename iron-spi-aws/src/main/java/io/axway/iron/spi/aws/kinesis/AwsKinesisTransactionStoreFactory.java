package io.axway.iron.spi.aws.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import io.axway.iron.spi.storage.TransactionStore;
import io.axway.iron.spi.storage.TransactionStoreFactory;

/**
 * Kinesis transaction store factory to build Kinesis TransactionStore.
 */
public class AwsKinesisTransactionStoreFactory implements TransactionStoreFactory {
    public static final String ACTIVE_STREAM_STATUS = "ACTIVE";

    private AmazonKinesis m_kinesisClient;

    /**
     * Create a KinesisTransactionStoreFactory with some properties set to configure Kinesis :
     *
     * @param accessKey - aws access key (optional+)
     * @param secretKey - aws secret key (optional+)
     * @param endpoint - kinesis port (optional*)
     * @param port - kinesis endpoint (optional*)
     * @param region - aws region (optional*)
     * (+) to configure the access, both access key and secret key must be provided.
     * (*) to configure the endpoint URL, the endpoint, the port and the region must be provided.
     */
    AwsKinesisTransactionStoreFactory(String accessKey, String secretKey, String endpoint, Integer port, String region) {
        m_kinesisClient = AwsKinesisUtils.buildKinesisClient(accessKey, secretKey, endpoint, port, region);
    }

    @Override
    public TransactionStore createTransactionStore(String storeName) {
        return new AwsKinesisTransactionStore(m_kinesisClient, storeName);
    }
}
