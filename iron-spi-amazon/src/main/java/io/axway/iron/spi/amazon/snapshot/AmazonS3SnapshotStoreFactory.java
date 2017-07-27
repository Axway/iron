package io.axway.iron.spi.amazon.snapshot;

import com.amazonaws.services.s3.AmazonS3Client;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.SnapshotStoreFactory;

public class AmazonS3SnapshotStoreFactory implements SnapshotStoreFactory {
    private final AmazonS3Client m_amazonS3Client;

    public AmazonS3SnapshotStoreFactory(AmazonS3Client amazonS3Client) {
        m_amazonS3Client = amazonS3Client;
    }

    @Override
    public SnapshotStore createSnapshotStore(String storeName) {
        return new AmazonS3SnapshotStore(m_amazonS3Client, storeName);
    }
}
