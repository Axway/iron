package io.axway.iron.spi.aws.s3;

import com.amazonaws.services.s3.AmazonS3;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.SnapshotStoreFactory;

/**
 * AWS S3 Snapshot Store Factory.
 */
public class AwsS3SnapshotStoreFactory implements SnapshotStoreFactory {
    private final String m_bucketName;
    private final AmazonS3 m_amazonS3;

    /**
     * Create a AwsS3SnapshotStoreFactory with some properties set to configure S3 and the bucket :
     *
     * @param accessKey - aws access key (optional+)
     * @param secretKey - aws secret key (optional+)
     * @param endpoint - s3 endpoint (optional*)
     * @param port - s3 port (optional*)
     * @param region - aws region (optional*)
     * @param bucketName S3 bucket name (mandatory)
     * (+) to configure the access, both access key and secret key must be provided.
     * (*) to configure the endpoint URL, the endpoint, the port and the region must be provided.
     */
    public AwsS3SnapshotStoreFactory(String accessKey, String secretKey, String endpoint, Integer port, String region, String bucketName) {
        m_amazonS3 = AwsS3Utils.buildS3Client(accessKey, secretKey, endpoint, port, region);
        m_bucketName = AwsS3Utils.checkBucketIsAccessible(m_amazonS3, bucketName);
    }

    @Override
    public SnapshotStore createSnapshotStore(String storeName) {
        return new AwsS3SnapshotStore(m_amazonS3, m_bucketName, storeName);
    }
}
