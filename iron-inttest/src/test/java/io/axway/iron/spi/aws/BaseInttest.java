package io.axway.iron.spi.aws;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.google.common.base.Preconditions;
import io.axway.iron.spi.aws.kinesis.AwsKinesisException;
import io.axway.iron.spi.aws.s3.AwsS3Exception;

import static io.axway.iron.spi.aws.AwsTestHelper.*;
import static io.axway.iron.spi.aws.AwsTestHelper.buildKinesisClient;
import static io.axway.iron.spi.aws.AwsUtils.performAmazonActionWithRetry;
import static io.axway.iron.spi.aws.kinesis.AwsKinesisUtils.*;

/**
 * To run these tests, localstack must be started with Kinesis and S3, and localstack profile must be enabled.
 */
public abstract class BaseInttest {

    private static final Logger LOG = LoggerFactory.getLogger(BaseInttest.class);
    private static final String AWS_KINESIS_STREAM_NAME_PREFIX = "condor-iron-transaction-store-";

    protected final Properties m_configuration = loadConfiguration("configuration.properties");

    @BeforeClass
    public void localTesting() {
        handleLocalStackConfigurationForLocalTesting(m_configuration);
    }

    protected String createRandomBucketName() {
        String bucketBaseName = "irontest-" + System.getProperty("user.name");
        return bucketBaseName + "-" + UUID.randomUUID();
    }

    protected String createRandomStoreName() {
        return "store" + "-" + UUID.randomUUID();
    }

    protected String createRandomDirectoryName() {
        return "directory" + "-" + UUID.randomUUID();
    }

    protected void createS3Bucket(String storeName) {
        AmazonS3 amazonS3 = buildS3Client(m_configuration);
        String region = m_configuration.getProperty(S3_REGION);
        if (region == null) {
            region = amazonS3.getRegionName();
        }
        Preconditions.checkState(region != null && !region.trim().isEmpty(),
                                 "Can't find aws region. Please consider setting it in configuration.properties with {} key", S3_REGION);
        createBucketIfNotExists(amazonS3, storeName, region);
    }

    /**
     * Create a bucket if not exists.
     *
     * @param amazonS3 Amazon S3 client
     * @param bucketName bucket name
     * @param region AWS region
     */
    private static void createBucketIfNotExists(AmazonS3 amazonS3, String bucketName, String region) {
        HeadBucketRequest headBucketRequest = new HeadBucketRequest(bucketName);
        try {
            amazonS3.headBucket(headBucketRequest);
        } catch (AmazonServiceException e) {
            if (e.getStatusCode() == 404) {
                CreateBucketRequest createBucketRequest = new CreateBucketRequest(bucketName, region);
                amazonS3.createBucket(createBucketRequest);
            } else {
                throw e;
            }
        }
    }

    protected void deleteS3Bucket(String storeName) {
        AmazonS3 amazonS3 = buildS3Client(m_configuration);
        deleteBucket(amazonS3, storeName);
    }

    /**
     * Delete a bucket after emptying it
     *
     * @param amazonS3 Amazon S3 client
     * @param bucketName bucket name
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/delete-or-empty-bucket.html#delete-bucket-sdk-java">AWS SDK Doc</a>
     */
    private static void deleteBucket(AmazonS3 amazonS3, String bucketName) {
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

    protected void createStreamAndWaitActivation(String storeName) {
        AmazonKinesis amazonKinesis = buildKinesisClient(m_configuration);
        String streamName = AWS_KINESIS_STREAM_NAME_PREFIX + storeName;
        ensureStreamExists(amazonKinesis, streamName, LOG);
    }

    protected void deleteKinesisStream(String storeName) {
        AmazonKinesis amazonKinesis = buildKinesisClient(m_configuration);
        String streamName = AWS_KINESIS_STREAM_NAME_PREFIX + storeName;
        deleteStream(amazonKinesis, streamName);
    }

    protected Path getIronSpiAwsInttestFilePath() {
        return Paths.get("iron", "iron-spi-aws-inttest");
    }

    public static Properties loadConfiguration(String resourceName) {
        // Try to read properties from fs
        try (FileInputStream fis = new FileInputStream(resourceName)) {
            Properties properties = new Properties();
            properties.load(fis);
            LOG.info("Configuration file {} is used", new File(resourceName).getAbsolutePath());
            return properties;
        } catch (FileNotFoundException e) {
            LOG.info("Configuration file {} not found, trying default location", new File(resourceName).getAbsolutePath());
        } catch (IOException e) {
            LOG.error("Can't read configuration file {}, error {}", new File(resourceName).getAbsolutePath(), e.getMessage());
        }

        // Try to read properties from classpath resources (i.e. to be used with maven's "localstack" profile)
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try {
            try (InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
                if (resourceStream == null) {
                    throw new IOException("Can't file configuration file on the classpath");
                }
                Properties properties = new Properties();
                properties.load(resourceStream);
                LOG.info("Configuration file {} is used", loader.getResource(resourceName).toExternalForm());
                return properties;
            }
        } catch (IOException e) {
            LOG.error("Configuration file {} not found in the classpath, AWS SDK client will be built with default configuration"
                              + " (Ideal to use on an ec2 instance to connect to aws infrastructure)."
                              + " Please consider using maven profile \"localstack\" to test locally with localstack", resourceName);
        }
        return new Properties();
    }

    /**
     * Delete stream silently
     * <p>
     * Won't raise an error if stream doesn't exists
     *
     * @param kinesis AmazonKinesis
     * @param streamName the name of the stream
     * @throws LimitExceededException after retry exhausted
     */
    private static void deleteStream(AmazonKinesis kinesis, String streamName) {
        String actionLabel = "deleteStream";
        performAmazonActionWithRetry(actionLabel, () -> {
            try {
                int httpStatusCode = kinesis.deleteStream(streamName).getSdkHttpMetadata().getHttpStatusCode();
                if (200 != httpStatusCode) {
                    throw new AwsKinesisException("Can't perform the action because the http status code not 200 ",
                                                  args -> args.add("storeName", streamName).add("action", actionLabel).add("httpStatusCode", httpStatusCode));
                }
            } catch (ResourceNotFoundException rnfe) {
                // No need to delete resource doesn't even exists
            }
            return null;
        }, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_DURATION_IN_MILLIS, LOG);
    }
}
