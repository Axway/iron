package io.axway.iron.spi.amazon.transaction;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import io.axway.iron.spi.storage.TransactionStore;
import io.axway.iron.spi.storage.TransactionStoreFactory;

public class AmazonKinesisTransactionStoreFactory implements TransactionStoreFactory {
    private final AmazonKinesisAsync m_amazonKinesis;
    private final String m_streamName;

    public AmazonKinesisTransactionStoreFactory(AmazonKinesisAsync amazonKinesis, String streamName) {
        m_amazonKinesis = amazonKinesis;
        m_streamName = streamName;
    }

    @Override
    public TransactionStore createTransactionStore(String storeName) {
        return new AmazonKinesisTransactionStore(m_amazonKinesis, m_streamName, storeName);
    }
}
