package io.axway.iron.spi.s3;

import com.amazonaws.services.s3.AmazonS3;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.SnapshotStoreFactory;

public class AmazonS3SnapshotStoreFactory implements SnapshotStoreFactory {

    private final String m_bucketName;
    private final AmazonS3 m_amazonS3;

    public AmazonS3SnapshotStoreFactory(AmazonS3 amazonS3, String bucketName) {
        m_amazonS3 = amazonS3;
        m_bucketName = bucketName;
    }

    @Override
    public SnapshotStore createSnapshotStore(String storeName) {
        return new AmazonS3SnapshotStore(m_amazonS3, m_bucketName, storeName);
    }
}
