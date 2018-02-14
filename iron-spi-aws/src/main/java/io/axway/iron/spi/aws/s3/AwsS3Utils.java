package io.axway.iron.spi.aws.s3;

import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.google.common.base.Throwables;

import static io.axway.iron.spi.aws.AwsProperties.*;
import static io.axway.iron.spi.aws.PropertiesHelper.getValue;
import static io.axway.iron.spi.aws.s3.AwsS3Properties.*;

public class AwsS3Utils {
    private static final Logger LOG = LoggerFactory.getLogger(AwsS3Utils.class);

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

    /**
     * Delete a bucket after emptying it
     *
     * @param s3
     * @param storeName
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/delete-or-empty-bucket.html#delete-bucket-sdk-java">AWS SDK Doc</a>
     */
    public static void deleteBucket(AmazonS3 s3, String storeName) {
        try {
            LOG.debug(" - removing objects from bucket");
            ObjectListing object_listing = s3.listObjects(storeName);
            while (true) {
                for (S3ObjectSummary summary : object_listing.getObjectSummaries()) {
                    s3.deleteObject(storeName, summary.getKey());
                }

                // more object_listing to retrieve?
                if (object_listing.isTruncated()) {
                    object_listing = s3.listNextBatchOfObjects(object_listing);
                } else {
                    break;
                }
            }

            LOG.debug(" - removing versions from bucket");
            try {
                VersionListing version_listing = s3.listVersions(new ListVersionsRequest().withBucketName(storeName));
                while (true) {
                    for (S3VersionSummary vs : version_listing.getVersionSummaries()) {
                        s3.deleteVersion(

                                storeName, vs.getKey(), vs.getVersionId());
                    }

                    if (version_listing.isTruncated()) {
                        version_listing = s3.listNextBatchOfVersions(version_listing);
                    } else {
                        break;
                    }
                }
            } catch (AmazonS3Exception e) {
                LOG.warn("Can't do s3.listVersions, don't care of this message if you are using localstack since this method is not implemented");
            }

            LOG.debug(" OK, bucket ready to delete!");
            s3.deleteBucket(storeName);
        } catch (AmazonServiceException e) {
            throw new RuntimeException("Can't remove s3 bucket " + storeName, e);
        }
    }
}
