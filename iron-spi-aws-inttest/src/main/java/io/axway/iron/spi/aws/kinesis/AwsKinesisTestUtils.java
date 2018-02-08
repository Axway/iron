package io.axway.iron.spi.aws.kinesis;

import java.util.*;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;

import static io.axway.iron.spi.aws.PropertiesHelper.*;
import static io.axway.iron.spi.aws.kinesis.AwsKinesisUtils.*;

public class AwsKinesisTestUtils {

    public static final String ACCESS_KEY = "AK";
    public static final String SECRET_KEY = "SK";
    public static final String REGION = "eu-west-1";
    public static final String KINESIS_ENDPOINT = "localhost";
    public static final Long KINESIS_PORT = 4568L;
    public static final String CLOUDWATCH_ENDPOINT = "localhost";
    public static final Long CLOUDWATCH_PORT = 4582L;


    public static KinesisTransactionStoreFactory buildTestAwsKinesisTransactionStoreFactory() {

        Properties properties = buildTestAwsKinesisProperties(ACCESS_KEY, SECRET_KEY, REGION, KINESIS_ENDPOINT, KINESIS_PORT, CLOUDWATCH_ENDPOINT,
                                                              CLOUDWATCH_PORT);
        return new KinesisTransactionStoreFactory(properties);
    }

    public static Properties buildTestAwsKinesisProperties(String accessKey, String secretKey, String region, String kinesisEndpoint, Long s3Port,
                                                           String cloudwatchEndpoint, Long cloudwatchPort) {
        Properties properties = new Properties();
        properties.setProperty(REGION_KEY, region);

        properties.setProperty(ACCESS_KEY_KEY, accessKey);
        properties.setProperty(SECRET_KEY_KEY, secretKey);
        properties.setProperty(KINESIS_ENDPOINT_KEY, kinesisEndpoint);
        properties.setProperty(KINESIS_PORT_KEY, s3Port.toString());
        properties.setProperty(CLOUDWATCH_ENDPOINT_KEY, cloudwatchEndpoint);
        properties.setProperty(CLOUDWATCH_PORT_KEY, cloudwatchPort.toString());

        properties.setProperty(DISABLE_VERIFY_CERTIFICATE_KEY, "");
        properties.setProperty(DISABLE_CBOR_KEY, "");

        return properties;
    }

    public static AmazonKinesis buildTestAmazonKinesis() {
        AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY));

        return buildKinesisConsumer(credentialsProvider, REGION, KINESIS_ENDPOINT, KINESIS_PORT);
    }

    public static void createStreamAndWaitActivation(String storeName) {
        AmazonKinesis amazonKinesis = buildTestAmazonKinesis();
        createStreamIfNotExists(amazonKinesis, storeName, 1);
        waitStreamActivation(amazonKinesis, storeName, 1000);
    }

    public static String createStreamAndWaitActivationWithRandomName() {
        String storeBaseName = "irontest-" + System.getProperty("user.name");
        String storeName = storeBaseName + "-" + UUID.randomUUID();
        createStreamAndWaitActivation(storeName);
        return storeName;
    }
}
