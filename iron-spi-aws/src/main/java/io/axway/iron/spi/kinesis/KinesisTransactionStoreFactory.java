package io.axway.iron.spi.kinesis;

import javax.annotation.*;
import io.axway.iron.spi.storage.TransactionStore;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Kinesis transaction store factory to build Kinesis TransactionStore.
 */
public class KinesisTransactionStoreFactory implements TransactionStoreFactory {

    private final String m_accessKey;
    private final String m_secretKey;

    @Nullable
    private String m_region;
    @Nullable
    private String m_kinesisEndpoint;
    @Nullable
    private Long m_kinesisPort;
    @Nullable
    private String m_cloudwatchEndpoint;
    @Nullable
    private Long m_cloudwatchPort;
    @Nullable
    private Boolean m_isVerifyCertificate;

    public KinesisTransactionStoreFactory(String accessKey, String secretKey) {
        m_accessKey = accessKey;
        m_secretKey = secretKey;
    }

    /**
     * This constructor allows to set region, kinesis endpoint and cloudwatch endpoint. Only useful for local testing.
     */
    KinesisTransactionStoreFactory(String accessKey, String secretKey, @Nullable String region, @Nullable String kinesisEndpoint, @Nullable Long kinesisPort,
                                   @Nullable String cloudwatchEndpoint, @Nullable Long cloudwatchPort, @Nullable Boolean isVerifyCertificate) {
        this(accessKey, secretKey);
        checkArgument((region == null && kinesisEndpoint == null && kinesisPort == null) || (region != null && kinesisEndpoint != null && kinesisPort != null),
                      "region, kinesisEndpoint and kinesisPort must all be null or all not null");
        m_region = region;
        m_kinesisEndpoint = kinesisEndpoint;
        m_kinesisPort = kinesisPort;
        m_cloudwatchEndpoint = cloudwatchEndpoint;
        m_cloudwatchPort = cloudwatchPort;
        m_isVerifyCertificate = isVerifyCertificate;
    }

    @Override
    public TransactionStore createTransactionStore(String storeName) {
        return new KinesisTransactionStore(m_accessKey, m_secretKey, storeName, m_region, m_kinesisEndpoint, m_kinesisPort, m_cloudwatchEndpoint,
                                           m_cloudwatchPort, m_isVerifyCertificate);
    }
}
