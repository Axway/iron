package io.axway.iron.spi.kinesis;

import javax.annotation.*;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;

import static com.google.common.base.Preconditions.checkArgument;

public class AwsKinesisUtils {

    public static final String ACTIVE_STREAM_STATUS = "ACTIVE";

    public static KinesisProducer buildKinesisProducer(AWSStaticCredentialsProvider credentialsProvider) {
        return new KinesisProducer(buildDefaultKinesisProducerConfiguration(credentialsProvider));
    }

    /**
     * Here'll walk through some of the config options and create an instance of
     * KinesisProducer, which will be used to put records.
     *
     * @return KinesisProducer instance used to put records.
     */
    public static KinesisProducer buildKinesisProducer(AWSStaticCredentialsProvider credentialsProvider, @Nullable String region,
                                                       @Nullable String kinesisEndpoint, @Nullable Long kinesisPort, @Nullable String cloudwatchEndpoint,
                                                       @Nullable Long cloudwatchPort, @Nullable Boolean isVerifyCertificate) {
        KinesisProducerConfiguration config = buildDefaultKinesisProducerConfiguration(credentialsProvider);
        if (region != null) {
            config.setRegion(region);
        }
        if (kinesisEndpoint != null) {
            config.setKinesisEndpoint(kinesisEndpoint);
        }
        if (kinesisPort != null) {
            config.setKinesisPort(kinesisPort);
        }
        if (cloudwatchEndpoint != null) {
            config.setCloudwatchEndpoint(cloudwatchEndpoint);
        }
        if (cloudwatchPort != null) {
            config.setCloudwatchPort(cloudwatchPort);
        }
        if (isVerifyCertificate != null) {
            config.setVerifyCertificate(isVerifyCertificate);
        }
        return new KinesisProducer(config);
    }

    private static KinesisProducerConfiguration buildDefaultKinesisProducerConfiguration(AWSStaticCredentialsProvider credentialsProvider) {
        //@formatter:on
        return new KinesisProducerConfiguration()
                //@formatter:off
                    .setCredentialsProvider(credentialsProvider)
                    // FIXME US CND-XXX this version of KinesisProducer does not support aggregation
                    .setAggregationEnabled(false);}

    /**
     * Build a Kinesis Consumer.
     */
    public static AmazonKinesis buildKinesisConsumer(AWSStaticCredentialsProvider credentialsProvider) {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        final AmazonKinesisClientBuilder builder = AmazonKinesisClient.builder().withClientConfiguration(clientConfiguration)
                .withCredentials(credentialsProvider);
        return builder.build();
    }

    /**
     * Build a Kinesis Consumer.
     */
    public static AmazonKinesis buildKinesisConsumer(AWSStaticCredentialsProvider credentialsProvider, @Nullable String region, @Nullable String kinesisEndpoint,
                                                     @Nullable Long kinesisPort) {
        checkArgument((region == null && kinesisEndpoint == null && kinesisPort == null) || (region != null && kinesisEndpoint != null && kinesisPort != null),
                      "region, kinesisEndpoint and kinesisPort must all be null or all not null");
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        final AmazonKinesisClientBuilder builder = AmazonKinesisClient.builder().withClientConfiguration(clientConfiguration)
                .withCredentials(credentialsProvider);
        if (region != null && kinesisEndpoint != null && kinesisPort != null) {
            String kinesisEndpointFull = "https://" + kinesisEndpoint + ":" + kinesisPort;
            builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(kinesisEndpointFull, region));
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
     * Returns true if the stream already exists.
     *
     * @param consumer
     * @param streamName the name of the stream
     */
    public static boolean isStreamActive(AmazonKinesis consumer, String streamName) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName).withLimit(1);
        DescribeStreamResult describeStreamResult;
        try {
            describeStreamResult = consumer.describeStream(describeStreamRequest);
        } catch (ResourceNotFoundException e) {
            return false;
        }
        return describeStreamResult.getStreamDescription().getStreamStatus().equals(ACTIVE_STREAM_STATUS);
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
