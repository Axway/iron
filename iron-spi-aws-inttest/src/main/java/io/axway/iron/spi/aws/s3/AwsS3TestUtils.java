package io.axway.iron.spi.aws.s3;

import java.util.*;
import com.amazonaws.services.s3.AmazonS3;

import static io.axway.iron.spi.aws.PropertiesHelper.*;

public class AwsS3TestUtils {

    public static String createRandomBucketName() {
        return "iron-bucket-" + UUID.randomUUID();
    }

    public static AwsS3SnapshotStoreFactory buildTestAwsS3SnapshotStoreFactory() {
        String bucketName = createRandomBucketName();
        String accessKey = "AK";
        String secretKey = "SK";
        String region = "eu-west-1";
        String s3Endpoint = "localhost";
        Long s3Port = 4572L;

        createS3BucketIfNotExist(bucketName, accessKey, secretKey, region, s3Endpoint, s3Port);

        Properties properties = buildTestAwsS3Properties(bucketName, accessKey, secretKey, region, s3Endpoint, s3Port);
        return new AwsS3SnapshotStoreFactory(properties);
    }

    public static Properties buildTestAwsS3Properties(String bucketName, String accessKey, String secretKey, String region, String s3Endpoint, Long s3Port) {
        Properties properties = new Properties();
        properties.setProperty(BUCKET_NAME_KEY, bucketName);
        properties.setProperty(ACCESS_KEY_KEY, accessKey);
        properties.setProperty(SECRET_KEY_KEY, secretKey);
        properties.setProperty(REGION_KEY, region);
        properties.setProperty(S3_ENDPOINT_KEY, s3Endpoint);
        properties.setProperty(S3_PORT_KEY, s3Port.toString());
        properties.setProperty(DISABLE_VERIFY_CERTIFICATE_KEY, "");
        return properties;
    }

    private static void createS3BucketIfNotExist(String bucketName, String accessKey, String secretKey, String region, String s3Endpoint, Long s3Port) {
        AmazonS3 amazonS3 = AwsS3Utils.buildS3Client(accessKey, secretKey, region, s3Endpoint, s3Port);
        if (!amazonS3.doesBucketExistV2(bucketName)) {
            amazonS3.createBucket(bucketName);
        }
    }
}
