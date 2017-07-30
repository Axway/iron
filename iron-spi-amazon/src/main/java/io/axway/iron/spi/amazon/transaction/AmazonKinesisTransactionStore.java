package io.axway.iron.spi.amazon.transaction;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import io.axway.iron.spi.storage.TransactionStore;

class AmazonKinesisTransactionStore implements TransactionStore {
    private final AmazonKinesisAsync m_amazonKinesis;
    private final String m_streamName;
    private final String m_storeName;

    private String m_shardIterator;

    private final AtomicLong m_txId = new AtomicLong();

    AmazonKinesisTransactionStore(AmazonKinesisAsync amazonKinesis, String streamName, String storeName) {
        m_amazonKinesis = amazonKinesis;
        m_streamName = streamName;
        m_storeName = storeName;
    }

    @Override
    public OutputStream createTransactionOutput() throws IOException {
        return new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();
                m_amazonKinesis.putRecordAsync(m_streamName, ByteBuffer.wrap(toByteArray()), m_storeName);
            }
        };
    }

    @Override
    public void seekTransactionPoll(long latestProcessedTransactionId) {
    }

    @Override
    public TransactionInput pollNextTransaction(long timeout, TimeUnit unit) {
        if (m_shardIterator == null) {
            m_shardIterator = m_amazonKinesis.getShardIterator(m_streamName, "shardId-000000000000", ShardIteratorType.LATEST.toString()).getShardIterator();
        }

        GetRecordsRequest request = new GetRecordsRequest();
        request.setShardIterator(m_shardIterator);
        request.setLimit(1);
        List<Record> records = m_amazonKinesis.getRecords(request).getRecords();
        if (records.isEmpty()) {
            return null;
        } else {
            Record record = records.get(0);
            byte[] array = record.getData().array();
            long txId = m_txId.incrementAndGet();
            return new TransactionInput() {
                @Override
                public InputStream getInputStream() throws IOException {
                    return new ByteArrayInputStream(array);
                }

                @Override
                public long getTransactionId() {
                    return txId;
                }
            };
        }
    }
}
