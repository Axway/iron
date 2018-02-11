package io.axway.iron.spi.aws.s3;

import java.util.*;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.google.common.base.Throwables;

import static io.axway.iron.spi.aws.AwsProperties.*;
import static io.axway.iron.spi.aws.PropertiesHelper.getValue;
import static io.axway.iron.spi.aws.s3.AwsS3Properties.*;

public class AwsS3Utils {

    public static AmazonS3 buildS3Client(Properties properties) {
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        Optional<String> accessKey = getValue(properties, ACCESS_KEY_KEY);
        Optional<String> secretKey = getValue(properties, SECRET_KEY_KEY);
        if (accessKey.isPresent() && secretKey.isPresent()) {
            builder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey.get(), secretKey.get())));
        }
        Optional<String> region = getValue(properties, REGION_KEY);
        Optional<String> s3Endpoint = getValue(properties, S3_ENDPOINT_KEY);
        Optional<String> s3Port = getValue(properties, S3_PORT_KEY);
        if (s3Endpoint.isPresent() && s3Port.isPresent() && region.isPresent()) {
            String s3EndpointFull = "https://" + s3Endpoint.get() + ":" + s3Port.get();
            builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3EndpointFull, region.get()));
        } else {
            region.ifPresent(builder::setRegion);
        }
        return builder.build();
    }

    /**
     * Throws an exception if the bucket does not exist or is not readable.
     *
     * @param amazonS3 Amazon S3 client
     * @param bucketName bucket name
     */
    public static String checkBucketIsAccessible(AmazonS3 amazonS3, String bucketName) {
        HeadBucketRequest headBucketRequest = new HeadBucketRequest(bucketName);
        try {
            amazonS3.headBucket(headBucketRequest);
        } catch (AmazonServiceException e) {
            throw new RuntimeException("Bucket " + bucketName + " is not accessible.", e);
        }
        return bucketName;
    }

    public static void createBucketIfNotExists(AmazonS3 amazonS3, String bucketName, String region) {
        HeadBucketRequest headBucketRequest = new HeadBucketRequest(bucketName);
        try {
            amazonS3.headBucket(headBucketRequest);
        } catch (AmazonServiceException e) {
            if (e.getStatusCode() == 404) {
                CreateBucketRequest createBucketRequest = new CreateBucketRequest(bucketName, region);
                amazonS3.createBucket(createBucketRequest);
            } else {
                throw Throwables.propagate(e);
            }
        }
    }
}
