package io.axway.iron.spi.s3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;

import static io.axway.iron.spi.s3.AwsS3Utils.createBucketIfNotExists;

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

        AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));

        AmazonS3 amazonS3 = AwsS3Utils.buildS3Client(credentialsProvider, region, s3Endpoint, s3Port);

        createBucketIfNotExists(amazonS3, bucketName, region);

        return new AmazonS3SnapshotStoreFactory(amazonS3, bucketName);
    }
}
