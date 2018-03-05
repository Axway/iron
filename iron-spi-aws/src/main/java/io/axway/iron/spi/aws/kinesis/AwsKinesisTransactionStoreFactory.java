package io.axway.iron.spi.aws.kinesis;

import java.util.*;
import com.amazonaws.services.kinesis.AmazonKinesis;
import io.axway.iron.spi.storage.TransactionStore;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static io.axway.iron.spi.aws.kinesis.AwsKinesisUtils.buildKinesisClient;

/**
 * Kinesis transaction store factory to build Kinesis TransactionStore.
 */
public class AwsKinesisTransactionStoreFactory implements TransactionStoreFactory {

    private AmazonKinesis m_kinesisClient;

    /**
     * Create a KinesisTransactionStoreFactory with some properties set to configure Kinesis :
     * - aws access key (optional+) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_ACCESS_KEY_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_ACCESS_KEY_ENVVAR}
     * - aws secret key (optional+) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_SECRET_KEY_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_SECRET_KEY_ENVVAR}
     * - aws region (optional*) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_REGION_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_REGION_ENVVAR}
     * - kinesis endpoint (optional*) {@value io.axway.iron.spi.aws.kinesis.AwsKinesisProperties.Constants#AWS_KINESIS_ENDPOINT_PROPERTY} / {@value io.axway.iron.spi.aws.kinesis.AwsKinesisProperties.Constants#AWS_KINESIS_ENDPOINT_ENVVAR}
     * - kinesis port (optional*) {@value io.axway.iron.spi.aws.kinesis.AwsKinesisProperties.Constants#AWS_KINESIS_PORT_PROPERTY} / {@value io.axway.iron.spi.aws.kinesis.AwsKinesisProperties.Constants#AWS_KINESIS_PORT_ENVVAR}
     * (+) to configure the access, both access key and secret key must be provided.
     * (*) to configure the endpoint URL, the endpoint, the port and the region must be provided.
     *
     * @param properties the properties
     */
    public AwsKinesisTransactionStoreFactory(Properties properties) {
        m_kinesisClient = buildKinesisClient(properties);
    }

    @Override
    public TransactionStore createTransactionStore(String storeName) {
        return new AwsKinesisTransactionStore(m_kinesisClient, storeName);
    }
}
