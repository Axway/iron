package io.axway.iron.spi.aws.kinesis;

import java.util.*;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

import static io.axway.iron.spi.aws.AwsProperties.*;
import static io.axway.iron.spi.aws.PropertiesHelper.getValue;
import static io.axway.iron.spi.aws.kinesis.AwsKinesisProperties.*;

public class AwsKinesisUtils {

    public static final String ACTIVE_STREAM_STATUS = "ACTIVE";

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
     * @param consumer
     * @param streamName the name of the stream
     */
    public static void createStreamIfNotExists(AmazonKinesis consumer, String streamName, int shardCount) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName).withLimit(1);
        try {
            consumer.describeStream(describeStreamRequest);
        } catch (ResourceNotFoundException e) {
            consumer.createStream(streamName, shardCount);
        }
    }

    /**
     * Returns true if the stream already exists.
     *
     * @param consumer
     * @param streamName the name of the stream
     */
    public static boolean doesStreamExist(AmazonKinesis consumer, String streamName) {
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
    public static void waitStreamActivation(AmazonKinesis consumer, String streamName, long streamCreationTimeout) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName).withLimit(1);
        DescribeStreamResult describeStreamResult = null;
        String streamStatus = null;
        long endTime = System.currentTimeMillis() + streamCreationTimeout;
        while (System.currentTimeMillis() < endTime) {
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
        }
        if (describeStreamResult == null || streamStatus == null || !streamStatus.equals(ACTIVE_STREAM_STATUS)) {
            throw new RuntimeException("Stream " + streamName + " never went " + ACTIVE_STREAM_STATUS);
        }
    }
}
