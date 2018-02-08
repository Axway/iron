package io.axway.iron.spi.aws.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.google.common.base.Throwables;

public class AwsS3Utils {

    public static AmazonS3 buildS3Client(String accessKey, String secretKey, String region, String s3Endpoint, Long s3Port) {
        AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
        return AwsS3Utils.buildS3Client(credentialsProvider, region, s3Endpoint, s3Port);
    }

    public static AmazonS3 buildS3Client(AWSStaticCredentialsProvider credentialsProvider, String region, String s3Endpoint, Long s3Port) {
        AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider);
        amazonS3ClientBuilder.withPathStyleAccessEnabled(true);
        String s3EndpointFull = "https://" + s3Endpoint + ":" + s3Port;
        amazonS3ClientBuilder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3EndpointFull, region));
        return amazonS3ClientBuilder.build();
    }

    /**
     * Throws an exception if the bucket does not exist or is not readable.
     *  @param amazonS3 Amazon S3 client
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
