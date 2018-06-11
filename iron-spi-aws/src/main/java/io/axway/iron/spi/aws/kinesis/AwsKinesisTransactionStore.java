package io.axway.iron.spi.aws.kinesis;

import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.*;
import javax.annotation.*;
import org.reactivestreams.Publisher;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import io.axway.alf.log.Logger;
import io.axway.alf.log.LoggerFactory;
import io.axway.iron.spi.StoreNamePrefixManagement;
import io.axway.iron.spi.storage.TransactionStore;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;

import static io.axway.alf.assertion.Assertion.checkState;
import static io.axway.iron.spi.StoreNamePrefixManagement.readStoreName;
import static io.axway.iron.spi.aws.AwsUtils.*;
import static io.axway.iron.spi.aws.kinesis.AwsKinesisUtils.*;
import static java.math.BigInteger.ZERO;
import static java.util.Collections.emptyList;

/**
 * Kinesis transaction store factory to build Kinesis TransactionStore.
 */
public class AwsKinesisTransactionStore implements TransactionStore {
    private static final Logger LOG = LoggerFactory.getLogger(AwsKinesisTransactionStore.class);

    private static final long INITIAL_MINIMAL_DURATION_BETWEEN_TWO_GET_SHARD_ITERATOR_REQUESTS = 1_000 / 4; // max 5 calls per second
    private static final String USELESS_PARTITION_KEY = "uselessPartitionKey";

    private final String m_streamName;
    private final Shard m_shard;
    private final AmazonKinesis m_kinesis;
    private BigInteger m_seekTransactionId = null;
    private String m_shardIterator;
    @Nullable
    private Long m_lastGetShardIteratorRequestTime = null;

    private final Flowable<TransactionInput> m_transactionsFlow;
    private final StoreNamePrefixManagement m_prefixManagement = new StoreNamePrefixManagement();

    private AtomicLong m_durationBetweenRequests = new AtomicLong(INITIAL_MINIMAL_DURATION_BETWEEN_TWO_GET_SHARD_ITERATOR_REQUESTS);

    /**
     * Create a KinesisTransactionStoreFactory with some properties set to configure Kinesis :
     *
     * @param accessKey aws access key (optional+)
     * @param secretKey aws secret key (optional+)
     * @param endpoint kinesis port (optional*)
     * @param port kinesis endpoint (optional*)
     * @param region aws region (optional*)
     * @param streamName stream name prefix
     * (+) to configure the access, both access key and secret key must be provided.
     * (*) to configure the endpoint URL, the endpoint, the port and the region must be provided.
     */
    AwsKinesisTransactionStore(String accessKey, String secretKey, String endpoint, Integer port, String region, String streamName) {
        m_kinesis = buildKinesisClient(accessKey, secretKey, endpoint, port, region);
        m_streamName = streamName;
        ensureStreamExists(m_kinesis, m_streamName);
        m_shard = getUniqueShard();

        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest().withStreamName(m_streamName)//
                .withShardIteratorType(ShardIteratorType.TRIM_HORIZON)//
                .withShardId(m_shard.getShardId());
        GetShardIteratorResult getShardIteratorResult = performAmazonActionWithRetry("describe stream " + m_streamName,
                                                                                     () -> m_kinesis.getShardIterator(getShardIteratorRequest),
                                                                                     DEFAULT_RETRY_COUNT, DEFAULT_RETRY_COUNT);
        m_shardIterator = getShardIteratorResult.getShardIterator();

        m_transactionsFlow = Flowable                                                //
                .<List<Record>>generate(emitter -> emitter.onNext(nextRecords()))    //                                                                   //
                .subscribeOn(Schedulers.io())                                        //
                .observeOn(Schedulers.computation())                                 //
                .concatMap(Flowable::fromIterable)                                   //
                .map(record -> {
                    BigInteger seekTransactionId = new BigInteger(record.getSequenceNumber());
                    m_seekTransactionId = seekTransactionId;
                    ByteBuffer data = record.getData().asReadOnlyBuffer();
                    ByteBufferBackedInputStream inputStream = new ByteBufferBackedInputStream(data);
                    String storeName = readStoreName(inputStream);
                    return new TransactionInput() {
                        @Override
                        public String storeName() {
                            return storeName;
                        }

                        @Override
                        public InputStream getInputStream() {
                            return inputStream;
                        }

                        @Override
                        public BigInteger getTransactionId() {
                            return seekTransactionId;
                        }
                    };
                });
    }

    @Override
    public OutputStream createTransactionOutput(String storeName) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();
                ByteBuffer data = ByteBuffer.wrap(toByteArray());
                performAmazonActionWithRetry("describe stream " + m_streamName, () -> {
                    m_kinesis.putRecord(new PutRecordRequest().withStreamName(m_streamName).withData(data).withPartitionKey(USELESS_PARTITION_KEY));
                    return null;
                }, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_DURATION_IN_MILLIS);
            }
        };
        m_prefixManagement.writeNamePrefix(storeName, outputStream);
        return outputStream;
    }

    @Override
    public Publisher<TransactionInput> allTransactions() {
        return m_transactionsFlow;
    }

    @Override
    public void seekTransaction(BigInteger latestProcessedTransactionId) {
        m_seekTransactionId = latestProcessedTransactionId;
        if (!m_seekTransactionId.equals(ZERO)) { // the snapshot is not a special migration snapshot, some transactions have already been processed
            GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest().withStreamName(m_streamName)//
                    .withShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)//
                    .withShardId(m_shard.getShardId())//
                    .withStartingSequenceNumber(m_seekTransactionId.toString());
            GetShardIteratorResult getShardIteratorResult = performAmazonActionWithRetry("describe stream " + m_streamName,
                                                                                         () -> m_kinesis.getShardIterator(getShardIteratorRequest),
                                                                                         DEFAULT_RETRY_COUNT, DEFAULT_RETRY_DURATION_IN_MILLIS);
            m_shardIterator = getShardIteratorResult.getShardIterator();
        }
    }

    @Override
    public void close() {
        m_kinesis.shutdown();
    }

    /**
     * Returns the single created shard (stream has a single shard).
     */
    private Shard getUniqueShard() {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(m_streamName).withLimit(1);
        DescribeStreamResult describeStreamResult = performAmazonActionWithRetry("describe stream " + m_streamName,
                                                                                 () -> m_kinesis.describeStream(describeStreamRequest), DEFAULT_RETRY_COUNT,
                                                                                 DEFAULT_RETRY_DURATION_IN_MILLIS);
        String streamStatus = describeStreamResult.getStreamDescription().getStreamStatus();
        if (streamStatus == null || !streamStatus.equals(ACTIVE_STREAM_STATUS)) {
            throw new AwsKinesisException("Stream does not exist", args -> args.add("streamName", m_streamName));
        }
        List<Shard> shards = describeStreamResult.getStreamDescription().getShards();
        checkState(shards.size() == 1, "Kinesis Stream should contain only one shard",
                   args -> args.add("streamName", m_streamName).add("shardCount", shards.size()));
        return shards.get(0);
    }

    @Nullable
    private List<Record> nextRecords() {
        if (!waitTheMinimalDurationToExecuteTheNextProvisioningRequest()) {
            return emptyList();
        }
        // Suboptimal request, should store records in a list instead. To be fixed.
        GetRecordsRequest getRecordsRequest = new GetRecordsRequest().withShardIterator(m_shardIterator).withLimit(1);
        return getRecords(getRecordsRequest);
    }

    /**
     * Wait that the minimum duration between two GetShardIteratorRequests has elapsed.
     *
     * @return true if the duration elapsed, false if the thread has been interrupted
     */
    private boolean waitTheMinimalDurationToExecuteTheNextProvisioningRequest() {
        if (m_lastGetShardIteratorRequestTime != null) {
            long delay = m_durationBetweenRequests.get() - (System.currentTimeMillis() - m_lastGetShardIteratorRequestTime);
            if (delay > 0) {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        m_lastGetShardIteratorRequestTime = System.currentTimeMillis();
        return true;
    }

    /**
     * Retrieves records corresponding to the request.
     *
     * @param getRecordsRequest the request
     * @return records corresponding to the request
     */
    @Nullable
    private List<Record> getRecords(GetRecordsRequest getRecordsRequest) {
        return tryAmazonAction("", () -> {
            GetRecordsResult getRecordsResult = m_kinesis.getRecords(getRecordsRequest);
            m_shardIterator = getRecordsResult.getNextShardIterator();
            List<Record> records = getRecordsResult.getRecords();
            LOG.trace("Get records", args -> args.add("streamName", m_streamName).add("record number", records.size())
                    .add("millisBehindLatest", getRecordsResult.getMillisBehindLatest()));
            return records;
        }, m_durationBetweenRequests).orElse(emptyList());
    }

    /**
     * Simple {@link InputStream} implementation that exposes currently available content of a {@link ByteBuffer}.
     */
    private class ByteBufferBackedInputStream extends InputStream {
        private final ByteBuffer m_byteBuffer;

        private ByteBufferBackedInputStream(ByteBuffer byteBuffer) {
            m_byteBuffer = byteBuffer;
        }

        @Override
        public int available() {
            return m_byteBuffer.remaining();
        }

        @Override
        public int read() {
            return m_byteBuffer.hasRemaining() ? m_byteBuffer.get() & 255 : -1;
        }

        @Override
        public int read(byte[] bytes, int off, int len) {
            if (!m_byteBuffer.hasRemaining()) {
                return -1;
            }
            int newLen = Math.min(len, m_byteBuffer.remaining());
            m_byteBuffer.get(bytes, off, newLen);
            return newLen;
        }
    }
}
