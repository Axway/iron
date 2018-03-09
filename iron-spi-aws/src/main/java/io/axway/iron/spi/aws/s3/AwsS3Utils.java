package io.axway.iron.spi.aws.s3;

import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.HeadBucketRequest;

import static io.axway.iron.spi.aws.AwsProperties.*;
import static io.axway.iron.spi.aws.AwsUtils.setAws;

public class AwsS3Utils {
    private static final Logger LOG = LoggerFactory.getLogger(AwsS3Utils.class);

    /**
     * Create a AwsS3SnapshotStoreFactory with some properties set to configure S3 client:
     * - aws access key (optional+) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_ACCESS_KEY_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_ACCESS_KEY_ENVVAR}
     * - aws secret key (optional+) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_SECRET_KEY_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_SECRET_KEY_ENVVAR}
     * - aws region (optional*) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_REGION_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_REGION_ENVVAR}
     * - s3 endpoint (optional*) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_S3_ENDPOINT_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_S3_ENDPOINT_ENVVAR}
     * - s3 port (optional*) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_S3_PORT_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_S3_PORT_ENVVAR}
     * (+) to configure the access, both access key and secret key must be provided.
     * (*) to configure the endpoint URL, the endpoint, the port and the region must be provided.
     *
     * @param properties the properties
     */
    public static AmazonS3 buildS3Client(Properties properties) {
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        setAws(properties, builder, S3_ENDPOINT_KEY, S3_PORT_KEY);
        return builder.build();
    }

    /**
     * Throws an exception if the bucket does not exist or is not readable.
     *
     * @param amazonS3 Amazon S3 client
     * @param bucketName bucket name
     */
    static String checkBucketIsAccessible(AmazonS3 amazonS3, String bucketName) {
        HeadBucketRequest headBucketRequest = new HeadBucketRequest(bucketName);
        try {
            amazonS3.headBucket(headBucketRequest);
        } catch (AmazonServiceException e) {
            throw new AwsS3Exception("Bucket is not accessible", args -> args.add("bucketName", bucketName), e);
        }
        return bucketName;
    }

}
