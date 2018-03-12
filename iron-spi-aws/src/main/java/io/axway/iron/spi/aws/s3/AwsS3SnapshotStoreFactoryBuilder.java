package io.axway.iron.spi.aws.s3;

import java.util.function.*;
import javax.xml.bind.annotation.*;
import io.axway.iron.spi.storage.SnapshotStoreFactory;

@XmlRootElement
public class AwsS3SnapshotStoreFactoryBuilder implements Supplier<SnapshotStoreFactory> {
    private String m_accessKey;
    private String m_secretKey;

    private String m_endpoint;
    private Integer m_port;
    private String m_bucketName;
    private String m_region;

    public void setAccessKey(String accessKey) {
        m_accessKey = accessKey;
    }

    public void setSecretKey(String secretKey) {
        m_secretKey = secretKey;
    }

    public void setEndpoint(String endpoint) {
        m_endpoint = endpoint;
    }

    public void setPort(Integer port) {
        m_port = port;
    }

    public void setBucketName(String bucketName) {
        m_bucketName = bucketName;
    }

    public void setRegion(String region) {
        m_region = region;
    }

    @Override
    public SnapshotStoreFactory get() {
        return new AwsS3SnapshotStoreFactory(m_accessKey, m_secretKey, m_endpoint, m_port, m_region, m_bucketName);
    }
}
