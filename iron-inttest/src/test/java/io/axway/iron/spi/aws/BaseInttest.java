package io.axway.iron.spi.aws;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.google.common.base.Preconditions;
import io.axway.iron.core.spi.file.FileStoreFactory;
import io.axway.iron.spi.aws.kinesis.AwsKinesisException;
import io.axway.iron.spi.aws.s3.AwsS3Utils;

import static com.amazonaws.SDKGlobalConfiguration.*;
import static io.axway.iron.spi.aws.AwsProperties.*;
import static io.axway.iron.spi.aws.kinesis.AwsKinesisUtils.*;

/**
 * To run these tests, localstack must be started with Kinesis and S3, and localstack profile must be enabled.
 */
public abstract class BaseInttest {
    private static final int MIN_3 = 1000 * 60 * 3;
    private static final Logger LOG = LoggerFactory.getLogger(BaseInttest.class);
    private static final int DEFAULT_RETRY_DURATION_IN_MILLIS = 5000;
    private static final int DEFAULT_RETRY_COUNT = 5;
    private static final String AWS_KINESIS_STREAM_NAME_PREFIX = "condor-iron-transaction-store-";

    protected final Properties m_configuration = loadConfiguration();

    @BeforeClass
    public void handleLocalStackConfigurationForLocalTesting() {
        if (PropertiesHelper.isSet(m_configuration, DISABLE_VERIFY_CERTIFICATE_KEY)) {
            System.setProperty(DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "");
        }
        if (PropertiesHelper.isSet(m_configuration, DISABLE_CBOR_KEY)) {
            System.setProperty(AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "");
        }
    }

    protected String createRandomStoreName() {
        String storeBaseName = "irontest-" + System.getProperty("user.name");
        return storeBaseName + "-" + UUID.randomUUID();
    }

    protected void createS3Bucket(String storeName) {
        AmazonS3 amazonS3 = AwsS3Utils.buildS3Client(m_configuration);
        String region = PropertiesHelper.getValue(m_configuration, REGION_KEY);
        if (region == null) {
            region = amazonS3.getRegionName();
        }
        Preconditions.checkState(region != null && !region.trim().isEmpty(),
                                 "Can't find aws region. Please consider setting it in configuration.properties with {} key", REGION_KEY.getPropertyKey());
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
        AmazonS3 amazonS3 = AwsS3Utils.buildS3Client(m_configuration);
        AwsS3Utils.deleteBucket(amazonS3, storeName);
    }

    protected void createStreamAndWaitActivation(String storeName) {
        AmazonKinesis amazonKinesis = buildKinesisClient(m_configuration);
        String streamName = AWS_KINESIS_STREAM_NAME_PREFIX + storeName;
        createStreamIfNotExists(amazonKinesis, streamName, 1);
        waitStreamActivation(amazonKinesis, streamName, MIN_3);
    }

    protected void deleteKinesisStream(String storeName) {
        AmazonKinesis amazonKinesis = buildKinesisClient(m_configuration);
        String streamName = AWS_KINESIS_STREAM_NAME_PREFIX + storeName;
        deleteStream(amazonKinesis, streamName);
    }

    protected FileStoreFactory buildFileStoreFactory() {
        return new FileStoreFactory(Paths.get("iron", "iron-spi-aws-inttest"));
    }

    protected FileStoreFactory buildFileStoreFactoryNoLimitedSize() {
        return new FileStoreFactory(Paths.get("iron", "iron-spi-aws-inttest"), null);
    }

    private Properties loadConfiguration() {
        String resourceName = "configuration.properties";

        // Try to read properties from fs
        try (FileInputStream fis = new FileInputStream(resourceName)) {
            Properties properties = new Properties();
            properties.load(fis);
            return properties;
        } catch (FileNotFoundException e) {
            LOG.warn("Configuration file {} not found, default configuration will be used", resourceName);
        } catch (IOException e) {
            LOG.error("Can't read configuration file {}, error {}", resourceName, e.getMessage());
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
                return properties;
            }
        } catch (IOException e) {
            LOG.warn(
                    "Configuration file {} not found in the classpath, AWS SDK client will be built with default configuration (Ideal to use on an ec2 instance to connect to aws infrastructure). Please consider using maven profile \"localstack\" to test locally with localstack",
                    resourceName);
        }
        return new Properties();
    }

    /**
     * Create a stream if it does not already exist.
     *
     * @param kinesis AmazonKinesis client
     * @param streamName the name of the stream
     * * @throws LimitExceededException after retry exhausted
     */
    private static void createStreamIfNotExists(AmazonKinesis kinesis, String streamName, int shardCount) {
        performAmazonActionWithRetry("createStream", () -> {
            DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName).withLimit(1);
            try {
                kinesis.describeStream(describeStreamRequest);
            } catch (ResourceNotFoundException e) {
                kinesis.createStream(streamName, shardCount);
            }
            return null;
        }, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_DURATION_IN_MILLIS);
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
        }, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_DURATION_IN_MILLIS);
    }

    /**
     * Waits that the stream has been created.
     */
    private static void waitStreamActivation(AmazonKinesis consumer, String streamName, long streamCreationTimeoutMillis) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName).withLimit(1);
        DescribeStreamResult describeStreamResult = null;
        String streamStatus = null;
        long endTime = System.currentTimeMillis() + streamCreationTimeoutMillis;
        do {
            try {
                describeStreamResult = consumer.describeStream(describeStreamRequest);
                streamStatus = describeStreamResult.getStreamDescription().getStreamStatus();
                if (streamStatus.equals(ACTIVE_STREAM_STATUS)) {
                    break;
                }
                try {
                    Thread.sleep(100);
                } catch (Exception ignored) {
                }
            } catch (ResourceNotFoundException ignored) {
            }
        } while (System.currentTimeMillis() < endTime);
        if (describeStreamResult == null || streamStatus == null || !streamStatus.equals(ACTIVE_STREAM_STATUS)) {
            throw new AwsKinesisException("Stream never went active",
                                          args -> args.add("streamName", streamName).add("streamCreationTimeoutMillis", streamCreationTimeoutMillis));
        }
    }

    /**
     * Handle retry for amazon quotas
     *
     * @param actionLabel action label used for logging purpose only
     * @param action the action to retry
     * @param retryLimit retry number limit
     * @param durationInMillis duration between each retry
     * @throws LimitExceededException after retry exhausted
     */
    private static void performAmazonActionWithRetry(String actionLabel, Supplier<Void> action, int retryLimit, int durationInMillis) {
        int retryCount = 0;
        do {
            try {
                action.get();
                return;
            } catch (LimitExceededException lee) {
                // We should just wait a little time before trying again
                LOG.debug("LimitExceededException while doing " + actionLabel + " will retry " + (retryLimit - retryCount) + " times");
            }
            try {
                Thread.sleep(durationInMillis);
                LOG.debug("Throttling {} for {} ms", actionLabel, durationInMillis);
            } catch (InterruptedException ignored) {
            }
        } while (retryCount++ < retryLimit);
        throw new LimitExceededException("Can't do " + actionLabel + " after " + retryLimit + " retries");
    }
}
