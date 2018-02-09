package io.axway.iron.spi.aws.kinesis;

import java.util.*;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import io.axway.iron.spi.aws.AwsProperties;

import static io.axway.iron.spi.aws.AwsTestUtils.getValue;
import static io.axway.iron.spi.aws.kinesis.AwsKinesisUtils.*;

public class AwsKinesisTestUtils implements AwsProperties, AwsKinesisProperties {

    public static final String REGION = getValue(REGION_KEY, "eu-west-1");

    public static final String ACCESS_KEY = getValue(ACCESS_KEY_KEY, "AK");
    public static final String SECRET_KEY = getValue(SECRET_KEY_KEY, "SK");

    public static final String KINESIS_ENDPOINT = getValue(KINESIS_ENDPOINT_KEY, "localhost");
    public static final String KINESIS_PORT = getValue(KINESIS_PORT_KEY, "4568");

    public static final String CLOUDWATCH_ENDPOINT = getValue(CLOUDWATCH_ENDPOINT_KEY, "localhost");
    public static final String CLOUDWATCH_PORT = getValue(CLOUDWATCH_PORT_KEY, "4582");

    public static final String DISABLE_VERIFY_CERTIFICATE = getValue(DISABLE_VERIFY_CERTIFICATE_KEY, "true");
    public static final String DISABLE_CBOR = getValue(DISABLE_CBOR_KEY, "true");

    public static KinesisTransactionStoreFactory buildTestAwsKinesisTransactionStoreFactory() {

        Properties properties = buildTestAwsKinesisProperties(ACCESS_KEY, SECRET_KEY, REGION, KINESIS_ENDPOINT, KINESIS_PORT, CLOUDWATCH_ENDPOINT,
                                                              CLOUDWATCH_PORT, DISABLE_VERIFY_CERTIFICATE, DISABLE_CBOR);
        return new KinesisTransactionStoreFactory(properties);
    }

    public static Properties buildTestAwsKinesisProperties(String accessKey, String secretKey, String region, String kinesisEndpoint, String s3Port,
                                                           String cloudwatchEndpoint, String cloudwatchPort, String disableVerifyCertificate,
                                                           String disableCbor) {
        Properties properties = new Properties();
        properties.setProperty(REGION_KEY, region);

        properties.setProperty(ACCESS_KEY_KEY, accessKey);
        properties.setProperty(SECRET_KEY_KEY, secretKey);
        properties.setProperty(KINESIS_ENDPOINT_KEY, kinesisEndpoint);
        properties.setProperty(KINESIS_PORT_KEY, s3Port);
        properties.setProperty(CLOUDWATCH_ENDPOINT_KEY, cloudwatchEndpoint);
        properties.setProperty(CLOUDWATCH_PORT_KEY, cloudwatchPort);
        if (disableVerifyCertificate.toLowerCase().equals("true")) {
            properties.setProperty(DISABLE_VERIFY_CERTIFICATE_KEY, "");
        }
        if (disableCbor.toLowerCase().equals("true")) {
            properties.setProperty(DISABLE_CBOR_KEY, "");
        }

        return properties;
    }

    public static AmazonKinesis buildTestAmazonKinesis() {
        if (DISABLE_VERIFY_CERTIFICATE.toLowerCase().equals("true")) {
            System.setProperty(DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "");
        }
        if (DISABLE_CBOR.toLowerCase().equals("true")) {
            System.setProperty(DISABLE_CBOR_SYSTEM_PROPERTY, "");
        }
        AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY));

        return buildKinesisConsumer(credentialsProvider, REGION, KINESIS_ENDPOINT, Long.valueOf(KINESIS_PORT));
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
