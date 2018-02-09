package io.axway.iron.spi.aws.s3;

import java.util.*;
import com.amazonaws.services.s3.AmazonS3;
import io.axway.iron.spi.aws.AwsProperties;

import static io.axway.iron.spi.aws.AwsTestUtils.getValue;

public class AwsS3TestUtils implements AwsProperties, AwsS3Properties {

    public static final String REGION = getValue(REGION_KEY, "eu-west-1");

    public static final String ACCESS_KEY = getValue(ACCESS_KEY_KEY, "AK");
    public static final String SECRET_KEY = getValue(SECRET_KEY_KEY, "SK");

    public static final String S3_ENDPOINT = getValue(S3_ENDPOINT_KEY, "localhost");
    public static final String S3_PORT = getValue(S3_PORT_KEY, "4572");

    public static final String DISABLE_VERIFY_CERTIFICATE = getValue(DISABLE_VERIFY_CERTIFICATE_KEY, "true");

    public static String createRandomBucketName() {
        return "iron-bucket-" + UUID.randomUUID();
    }

    public static AwsS3SnapshotStoreFactory buildTestAwsS3SnapshotStoreFactory() {
        String bucketName = createRandomBucketName();

        if (DISABLE_VERIFY_CERTIFICATE.toLowerCase().equals("true")) {
            System.setProperty(DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "");
        }

        createS3BucketIfNotExist(bucketName, ACCESS_KEY, SECRET_KEY, REGION, S3_ENDPOINT, Long.valueOf(S3_PORT));

        Properties properties = buildTestAwsS3Properties(bucketName, ACCESS_KEY, SECRET_KEY, REGION, S3_ENDPOINT, S3_PORT, DISABLE_VERIFY_CERTIFICATE);
        return new AwsS3SnapshotStoreFactory(properties);
    }

    public static Properties buildTestAwsS3Properties(String bucketName, String accessKey, String secretKey, String region, String s3Endpoint, String s3Port,
                                                      String disableVerifyCertificate) {
        Properties properties = new Properties();
        properties.setProperty(BUCKET_NAME_KEY, bucketName);
        properties.setProperty(ACCESS_KEY_KEY, accessKey);
        properties.setProperty(SECRET_KEY_KEY, secretKey);
        properties.setProperty(REGION_KEY, region);
        properties.setProperty(S3_ENDPOINT_KEY, s3Endpoint);
        properties.setProperty(S3_PORT_KEY, s3Port);
        if (disableVerifyCertificate.equals("true")) {
            properties.setProperty(DISABLE_VERIFY_CERTIFICATE_KEY, "");
        }
        return properties;
    }

    private static void createS3BucketIfNotExist(String bucketName, String accessKey, String secretKey, String region, String s3Endpoint, Long s3Port) {
        AmazonS3 amazonS3 = AwsS3Utils.buildS3Client(accessKey, secretKey, region, s3Endpoint, s3Port);
        if (!amazonS3.doesBucketExistV2(bucketName)) {
            amazonS3.createBucket(bucketName);
        }
    }
}
