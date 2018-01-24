package io.axway.iron.spi.s3;

import javax.annotation.*;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.google.common.base.Throwables;

import static com.google.common.base.Preconditions.checkArgument;

public class AwsS3Utils {

    public static AmazonS3 buildS3Client(AWSStaticCredentialsProvider credentialsProvider, @Nullable String region, @Nullable String s3Endpoint,
                                         @Nullable Long s3Port) {
        checkArgument((region == null && s3Endpoint == null && s3Port == null) || (region != null && s3Endpoint != null && s3Port != null),
                      "region, s3Endpoint and s3Port must all be null or all not null");
        AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider);
        amazonS3ClientBuilder.withPathStyleAccessEnabled(true);
        if (region != null && s3Endpoint != null && s3Port != null) {
            String s3EndpointFull = "https://" + s3Endpoint + ":" + s3Port;
            amazonS3ClientBuilder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3EndpointFull, region));
        }
        return amazonS3ClientBuilder.build();
    }

    public static boolean doesBucketExist(AmazonS3 amazonS3, String bucketName) {
        HeadBucketRequest headBucketRequest = new HeadBucketRequest(bucketName);
        try {
            amazonS3.headBucket(headBucketRequest);
        } catch (AmazonServiceException e) {
            if (e.getStatusCode() == 404) {
                return false;
            }
            throw Throwables.propagate(e);
        }
        return true;
    }

    public static void createBucketIfNotExists(AmazonS3 amazonS3, String bucketName, @Nullable String region) {
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
