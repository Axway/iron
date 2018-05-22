package io.axway.iron.spi.aws.kinesis;

import java.util.function.*;
import javax.annotation.*;
import io.axway.iron.spi.storage.TransactionStore;

import static io.axway.alf.assertion.Assertion.checkNotNullNorEmpty;

public class AwsKinesisTransactionStoreBuilder implements Supplier<TransactionStore> {
    private String m_accessKey;
    private String m_secretKey;

    private String m_endpoint;
    private Integer m_port;
    private String m_region;

    private String m_streamNamePrefix;

    private final String m_name;

    public AwsKinesisTransactionStoreBuilder(String name) {
        m_name = name.trim();
        checkNotNullNorEmpty(m_name, "Store name can't be null or empty");
    }

    public AwsKinesisTransactionStoreBuilder setAccessKey(@Nullable String accessKey) {
        m_accessKey = accessKey;
        return this;
    }

    public AwsKinesisTransactionStoreBuilder setSecretKey(@Nullable String secretKey) {
        m_secretKey = secretKey;
        return this;
    }

    public AwsKinesisTransactionStoreBuilder setEndpoint(@Nullable String endpoint) {
        m_endpoint = endpoint;
        return this;
    }

    public AwsKinesisTransactionStoreBuilder setPort(@Nullable Integer port) {
        m_port = port;
        return this;
    }

    public AwsKinesisTransactionStoreBuilder setRegion(@Nullable String region) {
        m_region = region;
        return this;
    }

    public AwsKinesisTransactionStoreBuilder setStreamNamePrefix(String streamNamePrefix) {
        m_streamNamePrefix = streamNamePrefix;
        return this;
    }

    @Override
    public TransactionStore get() {
        String streamName = (m_streamNamePrefix != null ? m_streamNamePrefix : "") + m_name;
        return new AwsKinesisTransactionStore(m_accessKey, m_secretKey, m_endpoint, m_port, m_region, streamName);
    }
}
