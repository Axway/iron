package io.axway.iron.spi.aws.s3;

import java.util.function.*;
import javax.annotation.*;
import javax.xml.bind.annotation.*;
import io.axway.iron.spi.storage.SnapshotStoreFactory;

@XmlRootElement
public class AwsS3SnapshotStoreFactoryBuilder implements Supplier<SnapshotStoreFactory> {
    private String m_accessKey;
    private String m_secretKey;

    private String m_endpoint;
    private Integer m_port;
    private String m_bucketName;
    private String m_directoryName;
    private String m_region;

    public AwsS3SnapshotStoreFactoryBuilder setAccessKey(@Nullable String accessKey) {
        m_accessKey = accessKey;
        return this;
    }

    public AwsS3SnapshotStoreFactoryBuilder setSecretKey(@Nullable String secretKey) {
        m_secretKey = secretKey;
        return this;
    }

    public AwsS3SnapshotStoreFactoryBuilder setEndpoint(@Nullable String endpoint) {
        m_endpoint = endpoint;
        return this;
    }

    public AwsS3SnapshotStoreFactoryBuilder setPort(@Nullable Integer port) {
        m_port = port;
        return this;
    }

    public AwsS3SnapshotStoreFactoryBuilder setBucketName(String bucketName) {
        m_bucketName = bucketName;
        return this;
    }

    public AwsS3SnapshotStoreFactoryBuilder setDirectoryName(String directoryName) {
        m_directoryName = directoryName;
        return this;
    }

    public AwsS3SnapshotStoreFactoryBuilder setRegion(@Nullable String region) {
        m_region = region;
        return this;
    }

    @Override
    public SnapshotStoreFactory get() {
        return new AwsS3SnapshotStoreFactory(m_accessKey, m_secretKey, m_endpoint, m_port, m_region, m_bucketName, m_directoryName);
    }
}
