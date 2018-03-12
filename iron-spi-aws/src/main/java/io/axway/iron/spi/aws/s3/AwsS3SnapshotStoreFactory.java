package io.axway.iron.spi.aws.s3;

import java.util.*;
import com.amazonaws.services.s3.AmazonS3;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.SnapshotStoreFactory;

import static com.google.common.base.Preconditions.checkState;
import static io.axway.iron.spi.aws.AwsProperties.*;
import static io.axway.iron.spi.aws.PropertiesHelper.getValue;
import static io.axway.iron.spi.aws.s3.AwsS3Utils.*;

/**
 * AWS S3 Snapshot Store Factory.
 */
public class AwsS3SnapshotStoreFactory implements SnapshotStoreFactory {

    private final String m_bucketName;
    private final AmazonS3 m_amazonS3;
    private final String m_directoryName;

    /**
     * Create a AwsS3SnapshotStoreFactory with some properties set to configure S3 and the bucket :
     * - AWS access key (optional+) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_ACCESS_KEY_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_ACCESS_KEY_ENVVAR}
     * - AWS secret key (optional+) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_SECRET_KEY_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_SECRET_KEY_ENVVAR}
     * - AWS region (optional*) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_REGION_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_REGION_ENVVAR}
     * - S3 bucket name (mandatory) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_S3_BUCKET_NAME_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_S3_BUCKET_NAME_ENVVAR}
     * - S3 endpoint (optional*) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_S3_ENDPOINT_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_S3_ENDPOINT_ENVVAR}
     * - S3 port (optional*) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_S3_PORT_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_S3_PORT_ENVVAR}
     * - S3 directory prefix (optional) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_S3_DIRECTORY_NAME_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_S3_DIRECTORY_NAME_ENVVAR}
     * (+) to configure the access, both access key and secret key must be provided.
     * (*) to configure the endpoint URL, the endpoint, the port and the region must be provided.
     *
     * @param properties the properties
     */
    public AwsS3SnapshotStoreFactory(Properties properties) {
        String bucketName = getValue(properties, S3_BUCKET_NAME_KEY);
        checkState(bucketName != null && !bucketName.trim().isEmpty(), "The bucket name [%s] should not be null or empty", bucketName);
        m_amazonS3 = buildS3Client(properties);
        m_bucketName = checkBucketIsAccessible(m_amazonS3, bucketName);
        m_directoryName = getValue(properties, S3_DIRECTORY_NAME_KEY);
        checkState(m_directoryName != null && !m_directoryName.trim().isEmpty(), "The directory name [%s] should not be null or empty", m_directoryName);
    }

    @Override
    public SnapshotStore createSnapshotStore(String storeName) {
        return new AwsS3SnapshotStore(m_amazonS3, m_bucketName, m_directoryName, storeName);
    }
}
