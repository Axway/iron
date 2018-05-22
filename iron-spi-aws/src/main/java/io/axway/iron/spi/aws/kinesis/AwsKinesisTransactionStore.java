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
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
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
import static io.axway.iron.spi.aws.AwsUtils.performAmazonActionWithRetry;
import static io.axway.iron.spi.aws.kinesis.AwsKinesisUtils.*;
import static java.math.BigInteger.ZERO;

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
    @Nullable
    private Long m_lastGetShardIteratorRequestTime = null;

    private final Flowable<TransactionInput> m_transactionsFlow;
    private final StoreNamePrefixManagement m_prefixManagement = new StoreNamePrefixManagement();

    private AtomicLong m_minimalDurationBetweenTwoGetShardIteratorRequests = new AtomicLong(INITIAL_MINIMAL_DURATION_BETWEEN_TWO_GET_SHARD_ITERATOR_REQUESTS);
    private static Random s_random = new Random();

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
                m_kinesis.putRecord(new PutRecordRequest().withStreamName(m_streamName).withData(data).withPartitionKey(USELESS_PARTITION_KEY));
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
        if (streamStatus == null || !streamStatus.equals(AwsKinesisUtils.ACTIVE_STREAM_STATUS)) {
            throw new AwsKinesisException("Stream does not exist", args -> args.add("streamName", m_streamName));
        }
        List<Shard> shards = describeStreamResult.getStreamDescription().getShards();
        checkState(shards.size() == 1, "Kinesis Stream should contain only one shard",
                   args -> args.add("streamName", m_streamName).add("shardCount", shards.size()));
        return shards.get(0);
    }

    private List<Record> nextRecords() {
        if (!waitTheMinimalDurationToExecuteTheNextGetShardIteratorRequest()) {
            return Collections.emptyList();
        }
        GetShardIteratorRequest getShardIteratorRequest;
        if (m_seekTransactionId == null || m_seekTransactionId
                .equals(ZERO)) { // First call to pollNextTransaction, no Snapshot has been created => retrieve the oldest element
            getShardIteratorRequest = new GetShardIteratorRequest().withStreamName(m_streamName)//
                    .withShardIteratorType(ShardIteratorType.TRIM_HORIZON)//
                    .withShardId(m_shard.getShardId());
        } else {// The m_seekTransactionId has already been set => retrieve elements after m_seekTransactionId
            getShardIteratorRequest = new GetShardIteratorRequest().withStreamName(m_streamName)//
                    .withShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)//
                    .withShardId(m_shard.getShardId())//
                    .withStartingSequenceNumber(m_seekTransactionId.toString());
        }
        GetShardIteratorResult getShardIteratorResult = tryGetShardIteratorRequest(getShardIteratorRequest);
        if (getShardIteratorResult == null) {
            return null;
        }
        // Suboptimal request, should store records in a list instead. To be fixed.
        GetRecordsRequest getRecordsRequest = new GetRecordsRequest().withShardIterator(getShardIteratorResult.getShardIterator()).withLimit(1);
        return getRecords(getRecordsRequest);
    }

    /**
     * Try to execute the getShardIteratorRequest.
     * If not allowed due to a ProvisionedThroughputExceededException, randomly increase the minimal duration between two GetShardIteratorRequests and return {@code null}.
     *
     * @param getShardIteratorRequest the GetShardIteratorRequest to execute
     * @return the GetShardIteratorResult if no ProvisionedThroughputExceededException occurs, else return {@code null}
     */
    @Nullable
    private GetShardIteratorResult tryGetShardIteratorRequest(GetShardIteratorRequest getShardIteratorRequest) {
        GetShardIteratorResult getShardIteratorResult;
        try {
            getShardIteratorResult = m_kinesis.getShardIterator(getShardIteratorRequest);
        } catch (ProvisionedThroughputExceededException e) {
            int durationRandomModifier = 1 + s_random.nextInt(64);// random duration to make readers out of sync, avoiding simultaneous readings
            long updatedDuration = m_minimalDurationBetweenTwoGetShardIteratorRequests.updateAndGet(duration -> duration * 2 // twice the duration
                    + duration * 2 / durationRandomModifier);// add random duration to avoid simultaneous reads
            LOG.debug("Update of minimal duration between two get shard iterator requests",
                      args -> args.add("streamName", m_streamName).add("new minimalDurationBetweenTwoGetShardIteratorRequests", updatedDuration));
            getShardIteratorResult = null;
        }
        return getShardIteratorResult;
    }

    /**
     * Wait that the minimum duration between two GetShardIteratorRequests has elapsed.
     *
     * @return true if the duration elapsed, false if the thread has been interrupted
     */
    private boolean waitTheMinimalDurationToExecuteTheNextGetShardIteratorRequest() {
        if (m_lastGetShardIteratorRequestTime != null) {
            long delay = m_minimalDurationBetweenTwoGetShardIteratorRequests.get() - (System.currentTimeMillis() - m_lastGetShardIteratorRequestTime);
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
    private List<Record> getRecords(GetRecordsRequest getRecordsRequest) {
        while (true) {
            try {
                GetRecordsResult getRecordsResult = m_kinesis.getRecords(getRecordsRequest);
                return getRecordsResult.getRecords();
            } catch (ProvisionedThroughputExceededException e) {
                LOG.debug("ProvisionedThroughputExceededException occurs", args -> args.add("streamName", m_streamName));
                // Too much The request rate for the stream is too high, or the requested data is too large for the available throughput. Wait to try again.
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AwsKinesisException("Interrupted while waiting provisioned throughput does no more exceed limit",
                                              args -> args.add("streamName", m_streamName).add("shardId", m_shard.getShardId()), e);
            }
        }
    }

    /**
     * Simple {@link InputStream} implementation that exposes currently available content of a {@link ByteBuffer}.
     */
    class ByteBufferBackedInputStream extends InputStream {
        final ByteBuffer m_byteBuffer;

        ByteBufferBackedInputStream(ByteBuffer byteBuffer) {
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
            len = Math.min(len, m_byteBuffer.remaining());
            m_byteBuffer.get(bytes, off, len);
            return len;
        }
    }
}
