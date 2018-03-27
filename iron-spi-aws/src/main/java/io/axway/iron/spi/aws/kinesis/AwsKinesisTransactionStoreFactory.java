package io.axway.iron.spi.aws.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import io.axway.iron.spi.storage.TransactionStore;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static io.axway.alf.assertion.Assertion.checkNotNullNorEmpty;
import static io.axway.iron.spi.aws.kinesis.AwsKinesisUtils.*;

/**
 * Kinesis transaction store factory to build Kinesis TransactionStore.
 */
public class AwsKinesisTransactionStoreFactory implements TransactionStoreFactory {

    private final AmazonKinesis m_kinesisClient;
    private final String m_streamNamePrefix;

    /**
     * Create a KinesisTransactionStoreFactory with some properties set to configure Kinesis :
     *
     * @param accessKey aws access key (optional+)
     * @param secretKey aws secret key (optional+)
     * @param endpoint kinesis port (optional*)
     * @param port kinesis endpoint (optional*)
     * @param region aws region (optional*)
     * @param streamNamePrefix stream name prefix
     * (+) to configure the access, both access key and secret key must be provided.
     * (*) to configure the endpoint URL, the endpoint, the port and the region must be provided.
     */
    AwsKinesisTransactionStoreFactory(String accessKey, String secretKey, String endpoint, Integer port, String region, String streamNamePrefix) {
        m_kinesisClient = buildKinesisClient(accessKey, secretKey, endpoint, port, region);
        m_streamNamePrefix = streamNamePrefix;
    }

    @Override
    public TransactionStore createTransactionStore(String storeName) {
        storeName = storeName.trim();
        checkNotNullNorEmpty(storeName, "Store name can't be null or empty");
        String streamName = (m_streamNamePrefix != null ? m_streamNamePrefix : "") + storeName;
        ensureStreamExists(m_kinesisClient, streamName);
        return new AwsKinesisTransactionStore(m_kinesisClient, streamName);
    }
}
