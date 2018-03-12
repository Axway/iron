package io.axway.iron.spi.aws;

import java.util.*;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.s3.AmazonS3;
import io.axway.iron.spi.aws.kinesis.AwsKinesisTransactionStoreFactory;
import io.axway.iron.spi.aws.kinesis.AwsKinesisUtils;
import io.axway.iron.spi.aws.s3.AwsS3SnapshotStoreFactory;
import io.axway.iron.spi.aws.s3.AwsS3Utils;

import static com.amazonaws.SDKGlobalConfiguration.*;

public class AwsTestHelper {
    public static final String KINESIS_ACCESS_KEY = "io.axway.iron.spi.aws.kinesis.access_key";
    public static final String KINESIS_SECRET_KEY = "io.axway.iron.spi.aws.kinesis.secret_key";
    public static final String KINESIS_ENDPOINT = "io.axway.iron.spi.aws.kinesis.endpoint";
    public static final String KINESIS_PORT = "io.axway.iron.spi.aws.kinesis.port";
    public static final String KINESIS_REGION = "io.axway.iron.spi.aws.kinesis.region";

    public static final String S3_ACCESS_KEY = "io.axway.iron.spi.aws.kinesis.access_key";
    public static final String S3_SECRET_KEY = "io.axway.iron.spi.aws.kinesis.secret_key";
    public static final String S3_ENDPOINT = "io.axway.iron.spi.aws.s3.endpoint";
    public static final String S3_PORT = "io.axway.iron.spi.aws.s3.port";
    public static final String S3_REGION = "io.axway.iron.spi.aws.s3.region";
    public static final String S3_BUCKET_NAME = "io.axway.iron.spi.aws.s3.bucket_name";

    public static final String DISABLE_VERIFY_CERTIFICATE = "io.axway.iron.spi.aws.disable_verify_certificate";
    public static final String DISABLE_CBOR = "io.axway.iron.spi.aws.kinesis.disable_cbor";

    public static AmazonKinesis buildKinesisClient(Properties configuration) {
        return AwsKinesisUtils.buildKinesisClient(configuration.getProperty(KINESIS_ACCESS_KEY), //
                                                  configuration.getProperty(KINESIS_SECRET_KEY), //
                                                  configuration.getProperty(KINESIS_ENDPOINT), //
                                                  Integer.valueOf(configuration.getProperty(KINESIS_PORT)), //
                                                  configuration.getProperty(KINESIS_REGION));
    }

    public static AmazonS3 buildS3Client(Properties configuration) {
        return AwsS3Utils.buildS3Client(configuration.getProperty(S3_ACCESS_KEY), //
                                        configuration.getProperty(S3_SECRET_KEY), //
                                        configuration.getProperty(S3_ENDPOINT), //
                                        Integer.valueOf(configuration.getProperty(S3_PORT)), //
                                        configuration.getProperty(S3_REGION));
    }

    public static AwsKinesisTransactionStoreFactory buildAwsKinesisTransactionStoreFactory(Properties configuration) {
        return new AwsKinesisTransactionStoreFactory(configuration.getProperty(KINESIS_ACCESS_KEY), //
                                                     configuration.getProperty(KINESIS_SECRET_KEY), //
                                                     configuration.getProperty(KINESIS_ENDPOINT), //
                                                     Integer.valueOf(configuration.getProperty(KINESIS_PORT)), //
                                                     configuration.getProperty(KINESIS_REGION));
    }

    public static AwsS3SnapshotStoreFactory buildAwsS3SnapshotStoreFactory(Properties configuration) {
        return new AwsS3SnapshotStoreFactory(configuration.getProperty(S3_ACCESS_KEY), //
                                             configuration.getProperty(S3_SECRET_KEY), //
                                             configuration.getProperty(S3_ENDPOINT), //
                                             Integer.valueOf(configuration.getProperty(S3_PORT)), //
                                             configuration.getProperty(S3_REGION), //
                                             configuration.getProperty(S3_BUCKET_NAME));
    }

    public static void handleLocalStackConfigurationForLocalTesting(Properties configuration) {
        if (configuration.get(DISABLE_VERIFY_CERTIFICATE) != null) {
            System.setProperty(DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "");
        }
        if (configuration.get(DISABLE_CBOR) != null) {
            System.setProperty(AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "");
        }
    }
}
