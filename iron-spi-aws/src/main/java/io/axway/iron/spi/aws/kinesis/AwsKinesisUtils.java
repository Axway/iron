package io.axway.iron.spi.aws.kinesis;

import java.util.*;
import java.util.function.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

import static io.axway.iron.spi.aws.AwsProperties.*;
import static io.axway.iron.spi.aws.PropertiesHelper.getValue;
import static io.axway.iron.spi.aws.kinesis.AwsKinesisProperties.*;

public class AwsKinesisUtils {
    static final String ACTIVE_STREAM_STATUS = "ACTIVE";

    private static final Logger LOG = LoggerFactory.getLogger(AwsKinesisUtils.class);
    private static final int DEFAULT_RETRY_DURATION_IN_MILLIS = 5000;
    private static final int DEFAULT_RETRY_COUNT = 5;

    public static AmazonKinesis buildKinesisClient(Properties properties) {
        AmazonKinesisClientBuilder builder = AmazonKinesisClientBuilder.standard();
        Optional<String> accessKey = getValue(properties, ACCESS_KEY_KEY);
        Optional<String> secretKey = getValue(properties, SECRET_KEY_KEY);
        if (accessKey.isPresent() && secretKey.isPresent()) {
            builder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey.get(), secretKey.get())));
        }
        Optional<String> region = getValue(properties, REGION_KEY);
        Optional<String> kinesisEndpoint = getValue(properties, KINESIS_ENDPOINT_KEY);
        Optional<String> kinesisPort = getValue(properties, KINESIS_PORT_KEY);
        if (kinesisEndpoint.isPresent() && kinesisPort.isPresent() && region.isPresent()) {
            String kinesisEndpointFull = "https://" + kinesisEndpoint.get() + ":" + kinesisPort.get();
            builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(kinesisEndpointFull, region.get()));
        } else {
            region.ifPresent(builder::setRegion);
        }
        return builder.build();
    }

    /**
     * Create a stream if it does not already exist.
     *
     * @param kinesis
     * @param streamName the name of the stream
     * * @throws LimitExceededException after retry exhausted
     */
    public static void createStreamIfNotExists(AmazonKinesis kinesis, String streamName, int shardCount) {
        performAmazonActionWithRetry("createStream", () -> {
            DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName).withLimit(1);
            try {
                kinesis.describeStream(describeStreamRequest);
            } catch (ResourceNotFoundException e) {
                kinesis.createStream(streamName, shardCount);
            }
            return null;
        }, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_DURATION_IN_MILLIS);
    }

    /**
     * Delete stream silently
     * <p>
     * Won't raise an error if stream doesn't exists
     *
     * @param kinesis
     * @param storeName
     * @throws LimitExceededException after retry exhausted
     */
    public static void deleteStream(AmazonKinesis kinesis, String storeName) {
        String actionLabel = "deleteStream";
        performAmazonActionWithRetry(actionLabel, () -> {
            try {
                int httpStatusCode = kinesis.deleteStream(storeName).getSdkHttpMetadata().getHttpStatusCode();
                if (200 != httpStatusCode) {
                    throw new RuntimeException("Can't perform " + actionLabel + " http status code not 200 " + httpStatusCode);
                }
            } catch (ResourceNotFoundException rnfe) {
                // No need to delete resource doesn't even exists
            }
            return null;
        }, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_DURATION_IN_MILLIS);
    }

    /**
     * Returns true if the stream already exists.
     *
     * @param consumer
     * @param streamName the name of the stream
     */
    static boolean doesStreamExist(AmazonKinesis consumer, String streamName) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName).withLimit(1);
        try {
            consumer.describeStream(describeStreamRequest);
        } catch (ResourceNotFoundException e) {
            return false;
        }
        return true;
    }

    /**
     * Returns the single created shard.
     */
    public static void waitStreamActivation(AmazonKinesis consumer, String streamName, long streamCreationTimeoutMillis) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName).withLimit(1);
        DescribeStreamResult describeStreamResult = null;
        String streamStatus = null;
        long endTime = System.currentTimeMillis() + streamCreationTimeoutMillis;
        do {
            try {
                describeStreamResult = consumer.describeStream(describeStreamRequest);
                streamStatus = describeStreamResult.getStreamDescription().getStreamStatus();
                if (streamStatus.equals(ACTIVE_STREAM_STATUS)) {
                    break;
                }
                try {
                    Thread.sleep(100);
                } catch (Exception ignored) {
                }
            } catch (ResourceNotFoundException ignored) {
            }
        } while (System.currentTimeMillis() < endTime);
        if (describeStreamResult == null || streamStatus == null || !streamStatus.equals(ACTIVE_STREAM_STATUS)) {
            throw new RuntimeException("Stream " + streamName + " never went " + ACTIVE_STREAM_STATUS);
        }
    }

    /**
     * Handle retry for amazon quotas
     *
     * @param actionLabel
     * @param action
     * @param retry
     * @param durationInMillis
     * @throws LimitExceededException after retry exhausted
     */
    private static void performAmazonActionWithRetry(String actionLabel, Supplier<Void> action, int retry, int durationInMillis) {
        int retryCount = 0;
        do {
            try {
                action.get();
                return;
            } catch (LimitExceededException lee) {
                // We should just wait a little time before trying again
                LOG.debug("LimitExceededException while doing " + actionLabel + " will retry " + (retry - retryCount) + " times");
            }
            try {
                Thread.sleep(durationInMillis);
                LOG.debug("Throttling {} for {} ms", actionLabel, durationInMillis);
            } catch (InterruptedException ignored) {
            }
        } while (retryCount++ < retry);
        throw new LimitExceededException("Can't do " + actionLabel + " after " + retry + " retries");
    }
}
