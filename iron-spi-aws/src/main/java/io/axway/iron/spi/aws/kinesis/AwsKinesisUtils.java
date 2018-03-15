package io.axway.iron.spi.aws.kinesis;

import java.util.*;
import org.slf4j.Logger;
import javax.annotation.*;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

import static io.axway.iron.spi.aws.AwsProperties.*;
import static io.axway.iron.spi.aws.AwsUtils.*;

/**
 * Some AWS Kinesis utils.
 */
public class AwsKinesisUtils {
    /**
     * Stream Status when the steam is active.
     */
    public static final String ACTIVE_STREAM_STATUS = "ACTIVE";

    public static final int DEFAULT_RETRY_DURATION_IN_MILLIS = 5000;
    public static final int DEFAULT_RETRY_COUNT = 5;

    private static final int MIN_3 = 1000 * 60 * 3;

    /**
     * Create a AmazonKinesis client configured with some optional properties (can also be configured using environment variables):
     * @param accessKey - aws access key (optional)
     * @param secretKey - aws secret key (optional)
     * @param endpoint - kinesis endpoint (optional*)
     * @param port - kinesis port (optional*)
     * @param region - aws region (optional*)
     * (*) to configure the endpoint, endpoint, port and region must be provided.
     *
     * @return a configured AmazonKinesis client
     */
    public static AmazonKinesis buildKinesisClient(@Nullable String accessKey, @Nullable String secretKey, //
                                                   @Nullable String endpoint, @Nullable Integer port, @Nullable String region) {
        AmazonKinesisClientBuilder builder = AmazonKinesisClientBuilder.standard();
        setAws(builder, accessKey, secretKey, endpoint, port, region);
        return builder.build();
    }

    /**
     * Ensure that the Kinesis Stream exists (creates it if does not exist).
     *
     * @param kinesisClient Kinesis client
     * @param streamName Stream name
     * @param logger logger
     */
    public static void ensureStreamExists(AmazonKinesis kinesisClient, String streamName, Logger logger) {
        createStreamIfNotExists(kinesisClient, streamName, 1, logger);
        waitStreamActivation(kinesisClient, streamName, MIN_3);
    }


    /**
     * Create a stream if it does not already exist.
     *  @param kinesis AmazonKinesis client
     * @param streamName the name of the stream
     * @param logger logger
     */
    private static void createStreamIfNotExists(AmazonKinesis kinesis, String streamName, int shardCount, Logger logger) {
        performAmazonActionWithRetry("createStream", () -> {
            DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName).withLimit(1);
            try {
                kinesis.describeStream(describeStreamRequest);
            } catch (ResourceNotFoundException e) {
                kinesis.createStream(streamName, shardCount);
            }
            return null;
        }, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_DURATION_IN_MILLIS, logger);
    }

    /**
     * Waits that the stream has been created.
     */
    private static void waitStreamActivation(AmazonKinesis consumer, String streamName, long streamCreationTimeoutMillis) {
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
}
