package io.axway.iron.spi.aws.s3;

import java.util.function.*;
import javax.annotation.*;
import javax.xml.bind.annotation.*;
import io.axway.iron.spi.storage.SnapshotStore;

@XmlRootElement
public class AwsS3SnapshotStoreBuilder implements Supplier<SnapshotStore> {
    private String m_accessKey;
    private String m_secretKey;

    private String m_endpoint;
    private Integer m_port;
    private String m_bucketName;
    private String m_directoryName;
    private String m_region;

    private final String m_name;

    public AwsS3SnapshotStoreBuilder(String name) {
        m_name = name;
    }

    public AwsS3SnapshotStoreBuilder setAccessKey(@Nullable String accessKey) {
        m_accessKey = accessKey;
        return this;
    }

    public AwsS3SnapshotStoreBuilder setSecretKey(@Nullable String secretKey) {
        m_secretKey = secretKey;
        return this;
    }

    public AwsS3SnapshotStoreBuilder setEndpoint(@Nullable String endpoint) {
        m_endpoint = endpoint;
        return this;
    }

    public AwsS3SnapshotStoreBuilder setPort(@Nullable Integer port) {
        m_port = port;
        return this;
    }

    public AwsS3SnapshotStoreBuilder setBucketName(String bucketName) {
        m_bucketName = bucketName;
        return this;
    }

    public AwsS3SnapshotStoreBuilder setDirectoryName(String directoryName) {
        m_directoryName = directoryName;
        return this;
    }

    public AwsS3SnapshotStoreBuilder setRegion(@Nullable String region) {
        m_region = region;
        return this;
    }

    @Override
    public SnapshotStore get() {
        return new AwsS3SnapshotStore(m_accessKey, m_secretKey, m_endpoint, m_port, m_region, m_bucketName, m_directoryName + "/" + m_name);
    }
}
