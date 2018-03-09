package io.axway.iron.spi.aws.kinesis;

import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.services.kinesis.AmazonKinesis;
import io.axway.iron.spi.storage.TransactionStore;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static io.axway.iron.spi.aws.AwsProperties.KINESIS_STREAM_NAME_PREFIX;
import static io.axway.iron.spi.aws.PropertiesHelper.getValue;
import static io.axway.iron.spi.aws.kinesis.AwsKinesisUtils.*;

/**
 * Kinesis transaction store factory to build Kinesis TransactionStore.
 */
public class AwsKinesisTransactionStoreFactory implements TransactionStoreFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AwsKinesisTransactionStoreFactory.class);

    private final AmazonKinesis m_kinesisClient;
    private final String m_kinesisStreamPrefix;

    /**
     * Create a KinesisTransactionStoreFactory with some properties set to configure Kinesis :
     * - aws access key (optional+) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_ACCESS_KEY_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_ACCESS_KEY_ENVVAR}
     * - aws secret key (optional+) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_SECRET_KEY_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_SECRET_KEY_ENVVAR}
     * - aws region (optional*) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_REGION_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_REGION_ENVVAR}
     * - kinesis endpoint (optional*) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_KINESIS_ENDPOINT_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_KINESIS_ENDPOINT_ENVVAR}
     * - kinesis port (optional*) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_KINESIS_PORT_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_KINESIS_PORT_ENVVAR}
     * - kinesis stream prefix (optional) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_KINESIS_STREAM_NAME_PREFIX_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_KINESIS_STREAM_NAME_PREFIX_ENVVAR}
     * (+) to configure the access, both access key and secret key must be provided.
     * (*) to configure the endpoint URL, the endpoint, the port and the region must be provided.
     *
     * @param properties the properties
     */
    public AwsKinesisTransactionStoreFactory(Properties properties) {
        m_kinesisClient = buildKinesisClient(properties);
        m_kinesisStreamPrefix = getValue(properties, KINESIS_STREAM_NAME_PREFIX);
    }

    @Override
    public TransactionStore createTransactionStore(String storeName) {
        checkArgument(!(storeName = storeName.trim()).isEmpty(), "Store name can't be null");
        String streamName = (m_kinesisStreamPrefix != null ? m_kinesisStreamPrefix : "") + storeName;
        ensureStreamExists(m_kinesisClient, streamName, LOG);
        return new AwsKinesisTransactionStore(m_kinesisClient, streamName);
    }
}
