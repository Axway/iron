package io.axway.iron.spi.aws.kinesis;

import java.util.*;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import io.axway.iron.spi.aws.AwsProperties;
import io.axway.iron.spi.storage.TransactionStore;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static io.axway.iron.spi.aws.PropertiesHelper.*;
import static io.axway.iron.spi.aws.kinesis.AwsKinesisUtils.*;

/**
 * Kinesis transaction store factory to build Kinesis TransactionStore.
 */
public class KinesisTransactionStoreFactory implements TransactionStoreFactory, AwsProperties, AwsKinesisProperties {

    private KinesisProducer m_producer;
    private AmazonKinesis m_consumer;

    public static final String AAA = "aaa";

    /**
     * Create a KinesisTransactionStoreFactory with some properties set:
     * {@value #REGION_KEY}
     * {@value #ACCESS_KEY_KEY}
     * {@value #SECRET_KEY_KEY}
     * {@value #KINESIS_ENDPOINT_KEY}
     * {@value #KINESIS_PORT_KEY}
     * {@value #CLOUDWATCH_ENDPOINT_KEY}
     * {@value #CLOUDWATCH_PORT_KEY}
     *
     * @param properties
     */
    public KinesisTransactionStoreFactory(Properties properties) {
        String region = checkKeyHasValue(properties, REGION_KEY);

        String accessKey = checkKeyHasValue(properties, ACCESS_KEY_KEY);
        String secretKey = checkKeyHasValue(properties, SECRET_KEY_KEY);

        String kinesisEndpoint = checkKeyHasValue(properties, KINESIS_ENDPOINT_KEY);
        Long kinesisPort = checkKeyHasLongValue(properties, KINESIS_PORT_KEY);

        String cloudwatchEndpoint = checkKeyHasValue(properties, CLOUDWATCH_ENDPOINT_KEY);
        Long cloudwatchPort = checkKeyHasLongValue(properties, CLOUDWATCH_PORT_KEY);

        boolean disableVerifyCertificate = manageDisableVerifyCertificate(properties);
        manageDisableCbor(properties);

        AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));

        m_producer = buildKinesisProducer(credentialsProvider, region, kinesisEndpoint, kinesisPort, cloudwatchEndpoint, cloudwatchPort, !disableVerifyCertificate);

        m_consumer = buildKinesisConsumer(credentialsProvider, region, kinesisEndpoint, kinesisPort);
    }

    @Override
    public TransactionStore createTransactionStore(String storeName) {
        return new KinesisTransactionStore(m_producer, m_consumer, storeName);
    }
}
