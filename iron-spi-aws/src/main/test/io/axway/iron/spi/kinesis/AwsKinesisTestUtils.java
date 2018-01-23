package io.axway.iron.spi.kinesis;

import javax.annotation.*;

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
        return new KinesisTransactionStoreFactory(accessKey, secretKey, region, kinesisEndpoint, kinesisPort, cloudwatchEndpoint, cloudwatchPort,
                                                  isVerifyCertificate);
    }
}
