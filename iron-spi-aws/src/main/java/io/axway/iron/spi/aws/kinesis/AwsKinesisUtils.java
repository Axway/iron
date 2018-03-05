package io.axway.iron.spi.aws.kinesis;

import java.util.*;
import java.util.function.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

import static io.axway.iron.spi.aws.AwsUtils.setAws;
import static io.axway.iron.spi.aws.kinesis.AwsKinesisProperties.*;

/**
 * Some AWS Kinesis utils.
 */
public class AwsKinesisUtils {
    /**
     * Stream Status when the steam is active.
     */
    static final String ACTIVE_STREAM_STATUS = "ACTIVE";

    private static final Logger LOG = LoggerFactory.getLogger(AwsKinesisUtils.class);
    private static final int DEFAULT_RETRY_DURATION_IN_MILLIS = 5000;
    private static final int DEFAULT_RETRY_COUNT = 5;

    /**
     * Create a AmazonKinesis client configured with some optional properties (can also be configured using environment variables):
     * - aws access key (optional) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_ACCESS_KEY_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_ACCESS_KEY_ENVVAR}
     * - aws secret key (optional) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_SECRET_KEY_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_SECRET_KEY_ENVVAR}
     * - aws region (optional*) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_REGION_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_REGION_ENVVAR}
     * - kinesis endpoint (optional*) {@value io.axway.iron.spi.aws.kinesis.AwsKinesisProperties.Constants#AWS_KINESIS_ENDPOINT_PROPERTY} / {@value io.axway.iron.spi.aws.kinesis.AwsKinesisProperties.Constants#AWS_KINESIS_ENDPOINT_ENVVAR}
     * - kinesis port (optional*) {@value io.axway.iron.spi.aws.kinesis.AwsKinesisProperties.Constants#AWS_KINESIS_PORT_PROPERTY} / {@value io.axway.iron.spi.aws.kinesis.AwsKinesisProperties.Constants#AWS_KINESIS_PORT_ENVVAR}
     * (*) to configure the endpoint, endpoint, port and region must be provided.
     *
     * @param properties properties to configure the AmazonKinesis client
     * @return a configured AmazonKinesis client
     */
    public static AmazonKinesis buildKinesisClient(Properties properties) {
        AmazonKinesisClientBuilder builder = AmazonKinesisClientBuilder.standard();
        setAws(properties, builder, KINESIS_ENDPOINT_KEY, KINESIS_PORT_KEY);
        return builder.build();
    }

    /**
     * Create a stream if it does not already exist.
     *
     * @param kinesis AmazonKinesis client
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
     * @param kinesis AmazonKinesis
     * @param streamName the name of the stream
     * @throws LimitExceededException after retry exhausted
     */
    public static void deleteStream(AmazonKinesis kinesis, String streamName) {
        String actionLabel = "deleteStream";
        performAmazonActionWithRetry(actionLabel, () -> {
            try {
                int httpStatusCode = kinesis.deleteStream(streamName).getSdkHttpMetadata().getHttpStatusCode();
                if (200 != httpStatusCode) {
                    throw new AwsKinesisException("Can't perform the action because the http status code not 200 ",
                                                  args -> args.add("storeName", streamName).add("action", actionLabel).add("httpStatusCode", httpStatusCode));
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
     * @param kinesis AmazonKinesis
     * @param streamName the name of the stream
     * @return true if the stream already exists
     */
    static boolean doesStreamExist(AmazonKinesis kinesis, String streamName) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName).withLimit(1);
        try {
            kinesis.describeStream(describeStreamRequest);
        } catch (ResourceNotFoundException e) {
            return false;
        }
        return true;
    }

    /**
     * Waits that the stream has been created.
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
            throw new AwsKinesisException("Stream never went active",
                                          args -> args.add("streamName", streamName).add("streamCreationTimeoutMillis", streamCreationTimeoutMillis));
        }
    }

    /**
     * Handle retry for amazon quotas
     *
     * @param actionLabel action label used for logging purpose only
     * @param action the action to retry
     * @param retryLimit retry number limit
     * @param durationInMillis duration between each retry
     * @throws LimitExceededException after retry exhausted
     */
    private static void performAmazonActionWithRetry(String actionLabel, Supplier<Void> action, int retryLimit, int durationInMillis) {
        int retryCount = 0;
        do {
            try {
                action.get();
                return;
            } catch (LimitExceededException lee) {
                // We should just wait a little time before trying again
                LOG.debug("LimitExceededException while doing " + actionLabel + " will retry " + (retryLimit - retryCount) + " times");
            }
            try {
                Thread.sleep(durationInMillis);
                LOG.debug("Throttling {} for {} ms", actionLabel, durationInMillis);
            } catch (InterruptedException ignored) {
            }
        } while (retryCount++ < retryLimit);
        throw new LimitExceededException("Can't do " + actionLabel + " after " + retryLimit + " retries");
    }
}
