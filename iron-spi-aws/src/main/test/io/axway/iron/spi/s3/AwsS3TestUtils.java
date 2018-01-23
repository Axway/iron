package io.axway.iron.spi.s3;

public class AwsS3TestUtils {

    public static AmazonS3SnapshotStoreFactory buildTestAwsS3SnapshotStoreFactory() {
        // Disable Cert checking to simplify testing (no need to manage certificates)
        System.setProperty("com.amazonaws.sdk.disableCertChecking", "");
        // Disable CBOR protocol which is not supported by kinesalite
        System.setProperty("com.amazonaws.sdk.disableCbor", "");

        String bucketName = "iron-bucket";
        String accessKey = "AK";
        String secretKey = "SK";
        String region = "eu-west-1";
        String s3Endpoint = "localhost";
        Long s3Port = 4572L;

        return new AmazonS3SnapshotStoreFactory(accessKey, secretKey, bucketName, region, s3Endpoint, s3Port);
    }
}
