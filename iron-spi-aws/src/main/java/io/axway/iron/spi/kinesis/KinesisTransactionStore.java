package io.axway.iron.spi.kinesis;

import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import io.axway.iron.spi.storage.TransactionStore;

import static com.google.common.base.Preconditions.*;

/**
 * Kinesis implementation of the TransactionStore.
 * Transactions are stored in a Kinesis Stream.
 */
class KinesisTransactionStore implements TransactionStore {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisTransactionStore.class);

    private static final int SHARD_COUNT = 1;// multi shard is not supported by this implementation
    private static final int STREAM_CREATION_DELAY = 60_000;// 1 minute
    private static final int DELAY_BETWEEN_STREAM_CREATION_REQUESTS = 100;// 100 ms
    private static final String ACTIVE_STREAM_STATUS = "ACTIVE";

    private final String m_streamName;
    private final Shard m_shard;
    private final KinesisProducer m_producer;
    private final AmazonKinesis m_consumer;

    private BigInteger m_seekTransactionId = null;

    public KinesisTransactionStore(KinesisProducer producer, AmazonKinesis consumer, String streamName) {
        checkArgument(!(m_streamName = streamName.trim()).isEmpty(), "Topic name can't be null");

        m_producer = producer;
        m_consumer = consumer;

        createStreamIfNotExists(m_streamName);

        m_shard = getUniqueShard();
    }

    /**
     * Create a stream if it does not already exist.
     *
     * @param streamName the name of the stream
     */
    private void createStreamIfNotExists(String streamName) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(m_streamName).withLimit(1);
        try {
            m_consumer.describeStream(describeStreamRequest);
        } catch (ResourceNotFoundException e) {
            m_consumer.createStream(streamName, SHARD_COUNT);
        }
    }

    /**
     * Returns the single created shard.
     */
    private Shard getUniqueShard() {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(m_streamName).withLimit(1);
        DescribeStreamResult describeStreamResult = null;
        String streamStatus = null;
        long endTime = System.currentTimeMillis() + STREAM_CREATION_DELAY;
        while (System.currentTimeMillis() < endTime) {
            try {
                describeStreamResult = m_consumer.describeStream(describeStreamRequest);
                streamStatus = describeStreamResult.getStreamDescription().getStreamStatus();
                if (streamStatus.equals(ACTIVE_STREAM_STATUS)) {
                    break;
                }
                try {
                    Thread.sleep(DELAY_BETWEEN_STREAM_CREATION_REQUESTS);
                } catch (Exception ignored) {
                }
            } catch (ResourceNotFoundException ignored) {
            }
        }
        if (describeStreamResult == null || streamStatus == null || !streamStatus.equals(ACTIVE_STREAM_STATUS)) {
            throw new RuntimeException("Stream " + m_streamName + " never went " + ACTIVE_STREAM_STATUS);
        }

        return describeStreamResult.getStreamDescription().getShards().get(0);
    }

    @Override
    public OutputStream createTransactionOutput() throws IOException {
        return new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();
                ByteBuffer wrap = ByteBuffer.wrap(toByteArray());
                m_producer.addUserRecord(m_streamName, "uselessPartitionKey", wrap);
            }
        };
    }

    @Override
    public void seekTransactionPoll(BigInteger latestProcessedTransactionId) {
        m_seekTransactionId = latestProcessedTransactionId;
    }

    @Override
    public TransactionInput pollNextTransaction(long timeout, TimeUnit unit) {
        GetShardIteratorRequest getShardIteratorRequest;
        if (m_seekTransactionId == null) {
            getShardIteratorRequest = new GetShardIteratorRequest().withStreamName(m_streamName)//
                    .withShardIteratorType(ShardIteratorType.TRIM_HORIZON)//
                    .withShardId(m_shard.getShardId());
        } else {
            getShardIteratorRequest = new GetShardIteratorRequest().withStreamName(m_streamName)//
                    .withShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)//
                    .withShardId(m_shard.getShardId())//
                    .withStartingSequenceNumber(m_seekTransactionId.toString());
        }
        GetShardIteratorResult getShardIteratorResult = m_consumer.getShardIterator(getShardIteratorRequest);

        GetRecordsRequest getRecordsRequest = new GetRecordsRequest().withShardIterator(getShardIteratorResult.getShardIterator()).withLimit(1);

        GetRecordsResult getRecordsResult = m_consumer.getRecords(getRecordsRequest);

        List<Record> records = null;
        do {
            try {
                records = getRecordsResult.getRecords();
            } catch (ProvisionedThroughputExceededException e) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException exception) {
                    throw new RuntimeException(exception);
                }
            }
        } while (records == null);

        if (records.isEmpty()) {
            return null;
        }

        checkState(records.size() == 1, "Kinesis should not return more than one record, and returns %d records", records.size());

        Record record = records.get(0);

        m_seekTransactionId = new BigInteger(record.getSequenceNumber());

        ByteBuffer data = record.getData().asReadOnlyBuffer();

        return new TransactionInput() {
            @Override
            public InputStream getInputStream() throws IOException {
                return new ByteBufferBackedInputStream(data);
            }

            @Override
            public BigInteger getTransactionId() {
                return new BigInteger(record.getSequenceNumber());
            }
        };
    }
}
