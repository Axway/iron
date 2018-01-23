package io.axway.iron.spi.s3;

import javax.annotation.*;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.SnapshotStoreFactory;

public class AmazonS3SnapshotStoreFactory implements SnapshotStoreFactory {

    private final String m_accessKey;
    private final String m_secretKey;
    private final String m_bucketName;
    @Nullable
    private String m_region;
    @Nullable
    private String m_s3Endpoint;
    @Nullable
    private Long m_s3Port;

    public AmazonS3SnapshotStoreFactory(String accessKey, String secretKey, String bucketName) {
        m_accessKey = accessKey;
        m_secretKey = secretKey;
        m_bucketName = bucketName;
    }

    AmazonS3SnapshotStoreFactory(String accessKey, String secretKey, String bucketName, @Nullable String region, @Nullable String s3Endpoint,
                                 @Nullable Long s3Port) {
        this(accessKey, secretKey, bucketName);
        m_region = region;
        m_s3Endpoint = s3Endpoint;
        m_s3Port = s3Port;
    }

    @Override
    public SnapshotStore createSnapshotStore(String storeName) {
        return new AmazonS3SnapshotStore(m_accessKey, m_secretKey, m_bucketName, storeName, m_region, m_s3Endpoint, m_s3Port);
    }
}
