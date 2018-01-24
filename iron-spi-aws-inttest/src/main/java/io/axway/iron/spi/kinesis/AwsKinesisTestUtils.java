package io.axway.iron.spi.kinesis;

import javax.annotation.*;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.producer.KinesisProducer;

public class AwsKinesisTestUtils {

    @Nonnull
    public static KinesisTransactionStoreFactory buildTestAwsKinesisTransactionStoreFactory() {
        // Disable Cert checking to simplify testing (no need to manage certificates)
        System.setProperty("com.amazonaws.sdk.disableCertChecking", "");
        // Disable CBOR protocol which is not supported by kinesalite
        System.setProperty("com.amazonaws.sdk.disableCbor", "");

        String accessKey = "AK";
        String secretKey = "SK";
        String region = "eu-west-1";
        String kinesisEndpoint = "localhost";
        Long kinesisPort = 4568L;
        String cloudwatchEndpoint = "localhost";
        Long cloudwatchPort = 4582L;
        // Disable certificate verification for testing purpose
        Boolean isVerifyCertificate = false;

        AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));

        KinesisProducer producer = AwsKinesisUtils
                .buildKinesisProducer(credentialsProvider, region, kinesisEndpoint, kinesisPort, cloudwatchEndpoint, cloudwatchPort, isVerifyCertificate);

        AmazonKinesis consumer = AwsKinesisUtils.buildKinesisConsumer(credentialsProvider, region, kinesisEndpoint, kinesisPort);

        return new KinesisTransactionStoreFactory(producer, consumer);
    }
}
