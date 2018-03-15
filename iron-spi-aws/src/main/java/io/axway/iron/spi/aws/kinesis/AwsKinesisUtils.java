package io.axway.iron.spi.aws.kinesis;

import javax.annotation.*;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

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
