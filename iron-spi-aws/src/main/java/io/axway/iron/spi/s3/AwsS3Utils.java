package io.axway.iron.spi.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.google.common.base.Throwables;

public class AwsS3Utils {

    public static AmazonS3 buildS3Client(AWSStaticCredentialsProvider credentialsProvider, String region, String s3Endpoint, Long s3Port) {
        AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider);
        amazonS3ClientBuilder.withPathStyleAccessEnabled(true);
        String s3EndpointFull = "https://" + s3Endpoint + ":" + s3Port;
        amazonS3ClientBuilder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3EndpointFull, region));
        return amazonS3ClientBuilder.build();
    }

    /**
     * Returns true if the bucket does exist and is readable.
     * Returns false if the bucket does not exist.
     * Throws an exception if the bucket is readable but is not readable.
     *
     * @param amazonS3 Amazon S3 client
     * @param bucketName bucket name
     * @return true if the bucket exists and
     */
    public static boolean isBucketAccessible(AmazonS3 amazonS3, String bucketName) {
        HeadBucketRequest headBucketRequest = new HeadBucketRequest(bucketName);
        try {
            amazonS3.headBucket(headBucketRequest);
            return true;
        } catch (AmazonServiceException e) {
            throw Throwables.propagate(e);
        }
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
