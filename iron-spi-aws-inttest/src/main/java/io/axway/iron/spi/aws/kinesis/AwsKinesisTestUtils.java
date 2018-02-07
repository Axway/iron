package io.axway.iron.spi.aws.kinesis;

import java.util.*;
import javax.annotation.*;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.producer.KinesisProducer;

import static io.axway.iron.spi.aws.kinesis.AwsKinesisUtils.*;

public class AwsKinesisTestUtils {

    public static final String ACCESS_KEY = "AK";
    public static final String SECRET_KEY = "SK";
    public static final String REGION = "eu-west-1";
    public static final String KINESIS_ENDPOINT = "localhost";
    public static final Long KINESIS_PORT = 4568L;
    public static final String CLOUDWATCH_ENDPOINT = "localhost";
    public static final Long CLOUDWATCH_PORT = 4582L;
    // Disable certificate verification for testing purpose
    public static final Boolean IS_VERIFY_CERTIFICATE = false;

    public static void setSystemPropertyForLocalstackKinesis() {
        // Disable Cert checking to simplify testing (no need to manage certificates)
        System.setProperty("com.amazonaws.sdk.disableCertChecking", "");
        // Disable CBOR protocol which is not supported by kinesalite
        System.setProperty("com.amazonaws.sdk.disableCbor", "");
    }

    @Nonnull
    public static KinesisTransactionStoreFactory buildTestAwsKinesisTransactionStoreFactory() {

        AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY));

        KinesisProducer producer = buildKinesisProducer(credentialsProvider, REGION, KINESIS_ENDPOINT, KINESIS_PORT, CLOUDWATCH_ENDPOINT, CLOUDWATCH_PORT,
                                                        IS_VERIFY_CERTIFICATE);

        AmazonKinesis consumer = buildKinesisConsumer(credentialsProvider, REGION, KINESIS_ENDPOINT, KINESIS_PORT);

        return new KinesisTransactionStoreFactory(producer, consumer);
    }

    @Nonnull
    public static AmazonKinesis buildTestAmazonKinesis() {
        AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY));

        return buildKinesisConsumer(credentialsProvider, REGION, KINESIS_ENDPOINT, KINESIS_PORT);
    }

    public static void createStreamAndWaitActivation(String storeName) {
        AmazonKinesis amazonKinesis = buildTestAmazonKinesis();
        createStreamIfNotExists(amazonKinesis, storeName, 1);
        waitStreamActivation(amazonKinesis, storeName, 1000);
    }

    @Nonnull
    public static String createStreamAndWaitActivationWithRandomName() {
        String storeBaseName = "irontest-" + System.getProperty("user.name");
        String storeName = storeBaseName + "-" + UUID.randomUUID();
        createStreamAndWaitActivation(storeName);
        return storeName;
    }
}
