package io.axway.iron.spi.aws.kinesis;

import java.util.function.*;
import javax.annotation.*;
import io.axway.iron.spi.storage.TransactionStoreFactory;

public class AwsKinesisTransactionStoreFactoryBuilder implements Supplier<TransactionStoreFactory> {
    private String m_accessKey;
    private String m_secretKey;

    private String m_endpoint;
    private Integer m_port;
    private String m_region;

    private String m_streamNamePrefix;

    public AwsKinesisTransactionStoreFactoryBuilder setAccessKey(@Nullable String accessKey) {
        m_accessKey = accessKey;
        return this;
    }

    public AwsKinesisTransactionStoreFactoryBuilder setSecretKey(@Nullable String secretKey) {
        m_secretKey = secretKey;
        return this;
    }

    public AwsKinesisTransactionStoreFactoryBuilder setEndpoint(@Nullable String endpoint) {
        m_endpoint = endpoint;
        return this;
    }

    public AwsKinesisTransactionStoreFactoryBuilder setPort(@Nullable Integer port) {
        m_port = port;
        return this;
    }

    public AwsKinesisTransactionStoreFactoryBuilder setRegion(@Nullable String region) {
        m_region = region;
        return this;
    }

    public AwsKinesisTransactionStoreFactoryBuilder setStreamNamePrefix(String streamNamePrefix) {
        m_streamNamePrefix = streamNamePrefix;
        return this;
    }

    @Override
    public TransactionStoreFactory get() {
        return new AwsKinesisTransactionStoreFactory(m_accessKey, m_secretKey, m_endpoint, m_port, m_region, m_streamNamePrefix);
    }
}
