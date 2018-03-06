package io.axway.iron.spi.aws.kinesis;

import java.util.*;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

import static io.axway.iron.spi.aws.AwsProperties.*;
import static io.axway.iron.spi.aws.AwsUtils.setAws;

/**
 * Some AWS Kinesis utils.
 */
public class AwsKinesisUtils {
    /**
     * Stream Status when the steam is active.
     */
    public static final String ACTIVE_STREAM_STATUS = "ACTIVE";

    /**
     * Create a AmazonKinesis client configured with some optional properties (can also be configured using environment variables):
     * - aws access key (optional) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_ACCESS_KEY_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_ACCESS_KEY_ENVVAR}
     * - aws secret key (optional) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_SECRET_KEY_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_SECRET_KEY_ENVVAR}
     * - aws region (optional*) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_REGION_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_REGION_ENVVAR}
     * - kinesis endpoint (optional*) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_KINESIS_ENDPOINT_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_KINESIS_ENDPOINT_ENVVAR}
     * - kinesis port (optional*) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_KINESIS_PORT_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_KINESIS_PORT_ENVVAR}
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
}
