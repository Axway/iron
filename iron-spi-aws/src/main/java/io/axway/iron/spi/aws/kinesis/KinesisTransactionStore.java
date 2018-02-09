package io.axway.iron.spi.aws.kinesis;

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
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import io.axway.iron.spi.storage.TransactionStore;

import static com.google.common.base.Preconditions.*;
import static io.axway.iron.spi.aws.kinesis.AwsKinesisUtils.doesStreamExist;

/**
 * Kinesis implementation of the TransactionStore.
 * Transactions are stored in a Kinesis Stream.
 */
class KinesisTransactionStore implements TransactionStore {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisTransactionStore.class);

    private final String m_streamName;
    private final Shard m_shard;
    private final KinesisProducer m_producer;
    private final AmazonKinesis m_consumer;

    private BigInteger m_seekTransactionId = null;

    /**
     * Create a KinesisTransactionStore based on an already active Kinesis Stream.
     * That is not the responsibility of this constructor to create the Kinesis Stream nor to wait that the Kinesis Stream reaches the 'ACTIVE" status.
     *
     * @param producer the kinesis producer
     * @param consumer the kinesis consumer
     * @param streamName the stream name
     */
    KinesisTransactionStore(KinesisProducer producer, AmazonKinesis consumer, String streamName) {
        checkArgument(!(m_streamName = streamName.trim()).isEmpty(), "Topic name can't be null");

        m_producer = producer;
        m_consumer = consumer;

        checkState(doesStreamExist(m_consumer, m_streamName), "The Kinesis Stream %s should already exist.", m_streamName);

        m_shard = getUniqueShard();
    }

    /**
     * Returns the single created shard (stream has a single shard).
     */
    private Shard getUniqueShard() {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(m_streamName).withLimit(1);
        DescribeStreamResult describeStreamResult = m_consumer.describeStream(describeStreamRequest);
        String streamStatus = describeStreamResult.getStreamDescription().getStreamStatus();
        if (streamStatus == null || !streamStatus.equals(AwsKinesisUtils.ACTIVE_STREAM_STATUS)) {
            throw new RuntimeException("Stream " + m_streamName + " does not exist.");
        }
        List<Shard> shards = describeStreamResult.getStreamDescription().getShards();

        checkState(shards.size() == 1, "This Kinesis Stream %s should contain a single Shard, but it contains %d shards.", m_streamName, shards.size());

        return shards.get(0);
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
        if (m_seekTransactionId == null) { // First call to pollNextTransaction, no Snapshot has been created => retrieve the oldest element
            getShardIteratorRequest = new GetShardIteratorRequest().withStreamName(m_streamName)//
                    .withShardIteratorType(ShardIteratorType.TRIM_HORIZON)//
                    .withShardId(m_shard.getShardId());
        } else {// The m_seekTransactionId has already been set => retrieve elements after m_seekTransactionId
            getShardIteratorRequest = new GetShardIteratorRequest().withStreamName(m_streamName)//
                    .withShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)//
                    .withShardId(m_shard.getShardId())//
                    .withStartingSequenceNumber(m_seekTransactionId.toString());
        }
        GetShardIteratorResult getShardIteratorResult = m_consumer.getShardIterator(getShardIteratorRequest);
        // Suboptimal request : fixed by https://techweb.axway.com/jira/browse/CND-592
        GetRecordsRequest getRecordsRequest = new GetRecordsRequest().withShardIterator(getShardIteratorResult.getShardIterator()).withLimit(1);

        List<Record> records = getRecords(getRecordsRequest);

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

    /**
     * Retrieves records corresponding to the request.
     *
     * @param getRecordsRequest the request
     * @return records corresponding to the request
     */
    private List<Record> getRecords(GetRecordsRequest getRecordsRequest) {
        while (true) {
            try {
                GetRecordsResult getRecordsResult = m_consumer.getRecords(getRecordsRequest);
                return getRecordsResult.getRecords();
            } catch (ProvisionedThroughputExceededException e) {
                // Too much The request rate for the stream is too high, or the requested data is too large for the available throughput. Wait to try again.
                try {
                    Thread.sleep(100);
                } catch (InterruptedException exception) {
                    throw new RuntimeException(exception);
                }
            }
        }
    }
}
