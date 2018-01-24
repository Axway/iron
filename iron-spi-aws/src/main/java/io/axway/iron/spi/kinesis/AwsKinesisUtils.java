package io.axway.iron.spi.kinesis;

import javax.annotation.*;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;

import static com.google.common.base.Preconditions.checkArgument;

public class AwsKinesisUtils {

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
}
