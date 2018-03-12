package io.axway.iron.spi.aws.kinesis;

import java.util.function.*;
import io.axway.iron.spi.storage.TransactionStoreFactory;

public class AwsKinesisTransactionStoreFactoryBuilder implements Supplier<TransactionStoreFactory> {
    private String m_accessKey;
    private String m_secretKey;

    private String m_endpoint;
    private Integer m_port;
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

    public void setRegion(String region) {
        m_region = region;
    }

    @Override
    public TransactionStoreFactory get() {
        return new AwsKinesisTransactionStoreFactory(m_accessKey, m_secretKey, m_endpoint, m_port, m_region);
    }
}
