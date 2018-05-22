package io.axway.iron.spi.aws.s3;

import javax.annotation.*;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.HeadBucketRequest;

import static io.axway.iron.spi.aws.AwsUtils.setAws;

public final class AwsS3Utils {

    /**
     * Create a AwsS3SnapshotStore with some properties set to configure S3 client:
     *
     * @param accessKey - aws access key (optional+)
     * @param secretKey - aws secret key (optional+)
     * @param endpoint - s3 endpoint (optional*)
     * @param port- s3 port (optional*)
     * @param region - aws region (optional*)
     * (+) to configure the access, both access key and secret key must be provided.
     * (*) to configure the endpoint URL, the endpoint, the port and the region must be provided.
     */
    public static AmazonS3 buildS3Client(@Nullable String accessKey, @Nullable String secretKey, //
                                         @Nullable String endpoint, @Nullable Integer port, @Nullable String region) {
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        setAws(builder, accessKey, secretKey, endpoint, port, region);
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

    private AwsS3Utils() {
        // utility class
    }
}
