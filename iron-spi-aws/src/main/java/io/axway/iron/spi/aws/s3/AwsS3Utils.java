package io.axway.iron.spi.aws.s3;

import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;

import static io.axway.iron.spi.aws.AwsUtils.setAws;
import static io.axway.iron.spi.aws.s3.AwsS3Properties.*;

public class AwsS3Utils {
    private static final Logger LOG = LoggerFactory.getLogger(AwsS3Utils.class);

    /**
     * Create a AwsS3SnapshotStoreFactory with some properties set to configure S3 client:
     * - aws access key (optional+) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_ACCESS_KEY_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_ACCESS_KEY_ENVVAR}
     * - aws secret key (optional+) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_SECRET_KEY_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_SECRET_KEY_ENVVAR}
     * - aws region (optional*) {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_REGION_PROPERTY} / {@value io.axway.iron.spi.aws.AwsProperties.Constants#AWS_REGION_ENVVAR}
     * - s3 endpoint (optional*) {@value io.axway.iron.spi.aws.s3.AwsS3Properties.Constants#AWS_S3_ENDPOINT_PROPERTY} / {@value io.axway.iron.spi.aws.s3.AwsS3Properties.Constants#AWS_S3_ENDPOINT_ENVVAR}
     * - s3 port (optional*) {@value io.axway.iron.spi.aws.s3.AwsS3Properties.Constants#AWS_S3_PORT_PROPERTY} / {@value io.axway.iron.spi.aws.s3.AwsS3Properties.Constants#AWS_S3_PORT_ENVVAR}
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

    /**
     * Delete a bucket after emptying it
     *
     * @param amazonS3 Amazon S3 client
     * @param bucketName bucket name
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/delete-or-empty-bucket.html#delete-bucket-sdk-java">AWS SDK Doc</a>
     */
    public static void deleteBucket(AmazonS3 amazonS3, String bucketName) {
        try {
            removeObjectsFromBucket(amazonS3, bucketName);
            removeVersionsFromBucket(amazonS3, bucketName);
            LOG.debug(" OK, bucket ready to delete!");
            amazonS3.deleteBucket(bucketName);
        } catch (AmazonServiceException e) {
            throw new AwsS3Exception("Can't remove s3 bucket", args -> args.add("storeName", bucketName), e);
        }
    }

    /**
     * Remove all versions from the bucket.
     *
     * @param amazonS3 Amazon S3 client
     * @param bucketName bucket name
     */
    private static void removeVersionsFromBucket(AmazonS3 amazonS3, String bucketName) {
        LOG.debug(" - removing versions from bucket");
        try {
            VersionListing versionListing = amazonS3.listVersions(new ListVersionsRequest().withBucketName(bucketName));
            while (true) {
                for (S3VersionSummary vs : versionListing.getVersionSummaries()) {
                    amazonS3.deleteVersion(bucketName, vs.getKey(), vs.getVersionId());
                }
                if (versionListing.isTruncated()) {
                    versionListing = amazonS3.listNextBatchOfVersions(versionListing);
                } else {
                    break;
                }
            }
        } catch (AmazonS3Exception e) {
            LOG.warn("Can't do s3.listVersions, don't care of this message if you are using localstack since this method is not implemented");
        }
    }

    /**
     * Remove all objects of the S3 bucket
     *
     * @param amazonS3 Amazon S3 client
     * @param bucketName bucket name
     */
    private static void removeObjectsFromBucket(AmazonS3 amazonS3, String bucketName) {
        LOG.debug(" - removing objects from bucket");
        ObjectListing objectListing = amazonS3.listObjects(bucketName);
        while (true) {
            for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
                amazonS3.deleteObject(bucketName, summary.getKey());
            }
            // more objectListing to retrieve?
            if (objectListing.isTruncated()) {
                objectListing = amazonS3.listNextBatchOfObjects(objectListing);
            } else {
                break;
            }
        }
    }
}
