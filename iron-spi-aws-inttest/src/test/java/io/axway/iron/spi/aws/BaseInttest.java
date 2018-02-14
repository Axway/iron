package io.axway.iron.spi.aws;

import java.io.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.s3.AmazonS3;
import com.google.common.base.Preconditions;
import io.axway.iron.spi.aws.kinesis.AwsKinesisUtils;
import io.axway.iron.spi.aws.s3.AwsS3Utils;

import static com.amazonaws.SDKGlobalConfiguration.*;
import static io.axway.iron.spi.aws.AwsProperties.REGION_KEY;

public abstract class BaseInttest {
    private static final int MIN_3 = 1000 * 60 * 3;
    protected final Properties m_configuration = loadConfiguration();
    private static final Logger LOG = LoggerFactory.getLogger(BaseInttest.class);

    @BeforeClass
    public void handleLocalStackConfigurationForLocalTesting() {
        if (PropertiesHelper.getValue(m_configuration, AwsProperties.DISABLE_VERIFY_CERTIFICATE_KEY).orElse("").toLowerCase().equals("true")) {
            System.setProperty(DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "");
        }
        if (PropertiesHelper.getValue(m_configuration, AwsProperties.DISABLE_CBOR_KEY).orElse("").toLowerCase().equals("true")) {
            System.setProperty(AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "");
        }
    }

    protected String createRandomStoreName() {
        String storeBaseName = "irontest-" + System.getProperty("user.name");
        return storeBaseName + "-" + UUID.randomUUID();
    }

    protected void createS3Bucket(String storeName) {
        AmazonS3 amazonS3 = AwsS3Utils.buildS3Client(m_configuration);
        String region = PropertiesHelper.getValue(m_configuration, REGION_KEY).orElseGet(amazonS3::getRegionName);
        Preconditions.checkState(region != null && !region.trim().isEmpty(),
                                 "Can't find aws region. Please consider setting it in configuration.properties with {} key",
                                 AwsProperties.REGION_KEY.getPropertyKey());
        AwsS3Utils.createBucketIfNotExists(amazonS3, storeName, region);
    }

    protected void deleteS3Bucket(String storeName) {
        AmazonS3 amazonS3 = AwsS3Utils.buildS3Client(m_configuration);
        AwsS3Utils.deleteBucket(amazonS3, storeName);
    }

    protected void createStreamAndWaitActivation(String storeName) {
        AmazonKinesis amazonKinesis = AwsKinesisUtils.buildKinesisClient(m_configuration);
        AwsKinesisUtils.createStreamIfNotExists(amazonKinesis, storeName, 1);
        AwsKinesisUtils.waitStreamActivation(amazonKinesis, storeName, MIN_3);
    }

    protected void deleteKinesisStream(String storeName) {
        AmazonKinesis amazonKinesis = AwsKinesisUtils.buildKinesisClient(m_configuration);
        AwsKinesisUtils.deleteStream(amazonKinesis, storeName);
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
}
