package io.axway.iron.spi.aws;

import java.util.*;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.s3.AmazonS3;
import io.axway.iron.spi.aws.kinesis.AwsKinesisTransactionStoreBuilder;
import io.axway.iron.spi.aws.kinesis.AwsKinesisUtils;
import io.axway.iron.spi.aws.s3.AwsS3SnapshotStoreBuilder;
import io.axway.iron.spi.aws.s3.AwsS3Utils;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

import static com.amazonaws.SDKGlobalConfiguration.*;

public class AwsTestHelper {
    public static final String KINESIS_ACCESS_KEY = "io.axway.iron.spi.aws.kinesis.access_key";
    public static final String KINESIS_SECRET_KEY = "io.axway.iron.spi.aws.kinesis.secret_key";
    public static final String KINESIS_ENDPOINT = "io.axway.iron.spi.aws.kinesis.endpoint";
    public static final String KINESIS_PORT = "io.axway.iron.spi.aws.kinesis.port";
    public static final String KINESIS_REGION = "io.axway.iron.spi.aws.kinesis.region";
    public static final String KINESIS_STREAM_NAME_PREFIX = "io.axway.iron.spi.aws.kinesis.stream_name_prefix";

    public static final String S3_ACCESS_KEY = "io.axway.iron.spi.aws.kinesis.access_key";
    public static final String S3_SECRET_KEY = "io.axway.iron.spi.aws.kinesis.secret_key";
    public static final String S3_ENDPOINT = "io.axway.iron.spi.aws.s3.endpoint";
    public static final String S3_PORT = "io.axway.iron.spi.aws.s3.port";
    public static final String S3_REGION = "io.axway.iron.spi.aws.s3.region";
    public static final String S3_BUCKET_NAME = "io.axway.iron.spi.aws.s3.bucket_name";
    public static final String S3_DIRECTORY_NAME = "io.axway.iron.spi.aws.s3.directory_name";

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

    public static TransactionStore buildAwsKinesisTransactionStoreFactory(String name, Properties configuration) {
        return new AwsKinesisTransactionStoreBuilder(name) //
                .setAccessKey(configuration.getProperty(KINESIS_ACCESS_KEY)) //
                .setSecretKey(configuration.getProperty(KINESIS_SECRET_KEY)) //
                .setEndpoint(configuration.getProperty(KINESIS_ENDPOINT)) //
                .setPort(Integer.valueOf(configuration.getProperty(KINESIS_PORT))) //
                .setRegion(configuration.getProperty(KINESIS_REGION)) //
                .setStreamNamePrefix(configuration.getProperty(KINESIS_STREAM_NAME_PREFIX)).get();
    }

    public static SnapshotStore buildAwsS3SnapshotStoreFactory(String name, Properties configuration) {
        return new AwsS3SnapshotStoreBuilder(name) //
                .setAccessKey(configuration.getProperty(S3_ACCESS_KEY)) //
                .setSecretKey(configuration.getProperty(S3_SECRET_KEY)) //
                .setEndpoint(configuration.getProperty(S3_ENDPOINT)) //
                .setPort(Integer.valueOf(configuration.getProperty(S3_PORT))) //
                .setRegion(configuration.getProperty(S3_REGION)) //
                .setBucketName(configuration.getProperty(S3_BUCKET_NAME)) //
                .setDirectoryName(configuration.getProperty(S3_DIRECTORY_NAME)) //
                .get();
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
