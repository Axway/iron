package io.axway.iron.spi.aws.s3;

import java.util.*;
import com.amazonaws.services.s3.AmazonS3;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.SnapshotStoreFactory;

import static com.google.common.base.Preconditions.checkState;
import static io.axway.iron.spi.aws.PropertiesHelper.getValue;
import static io.axway.iron.spi.aws.s3.AwsS3Properties.S3_BUCKET_NAME_KEY;
import static io.axway.iron.spi.aws.s3.AwsS3Utils.*;

/**
 * AWS S3 Snapshot Store Factory.
 */
public class AwsS3SnapshotStoreFactory implements SnapshotStoreFactory {

    private final String m_bucketName;
    private final AmazonS3 m_amazonS3;

    /**
     * Create a AwsS3SnapshotStoreFactory with some properties set to configure S3 and the bucket :
     * - aws access key (optional+) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_ACCESS_KEY_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_ACCESS_KEY_ENVVAR}
     * - aws secret key (optional+) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_SECRET_KEY_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_SECRET_KEY_ENVVAR}
     * - aws region (optional*) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_REGION_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_REGION_ENVVAR}
     * - s3 bucket name (mandatory) {@value io.axway.iron.spi.aws.s3.AwsS3Properties.Constants#AWS_S3_BUCKET_NAME_PROPERTY} / {@value io.axway.iron.spi.aws.s3.AwsS3Properties.Constants#AWS_S3_BUCKET_NAME_ENVVAR}
     * - s3 endpoint (optional*) {@value io.axway.iron.spi.aws.s3.AwsS3Properties.Constants#AWS_S3_ENDPOINT_PROPERTY} / {@value io.axway.iron.spi.aws.s3.AwsS3Properties.Constants#AWS_S3_ENDPOINT_ENVVAR}
     * - s3 port (optional*) {@value io.axway.iron.spi.aws.s3.AwsS3Properties.Constants#AWS_S3_PORT_PROPERTY} / {@value io.axway.iron.spi.aws.s3.AwsS3Properties.Constants#AWS_S3_PORT_ENVVAR}
     * (+) to configure the access, both access key and secret key must be provided.
     * (*) to configure the endpoint URL, the endpoint, the port and the region must be provided.
     *
     * @param properties the properties
     */
    public AwsS3SnapshotStoreFactory(Properties properties) {
        Optional<String> bucketName = getValue(properties, S3_BUCKET_NAME_KEY);
        checkState(bucketName.isPresent() && !bucketName.get().trim().isEmpty());
        m_amazonS3 = buildS3Client(properties);
        m_bucketName = checkBucketIsAccessible(m_amazonS3, bucketName.get());
    }

    @Override
    public SnapshotStore createSnapshotStore(String storeName) {
        return new AwsS3SnapshotStore(m_amazonS3, m_bucketName, storeName);
    }
}
