package io.axway.iron.spi.aws;

import java.util.*;
import java.util.function.*;
import org.slf4j.Logger;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.model.LimitExceededException;

import static io.axway.iron.spi.aws.AwsProperties.*;
import static io.axway.iron.spi.aws.PropertiesHelper.getValue;

public class AwsUtils {

    public static void setAws(Properties properties, AwsClientBuilder builder, AwsProperties endpointKey, AwsProperties portKey) {
        String accessKey = getValue(properties, ACCESS_KEY_KEY);
        String secretKey = getValue(properties, SECRET_KEY_KEY);
        if (accessKey != null && secretKey != null) {
            builder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)));
        }
        String region = getValue(properties, REGION_KEY);
        String s3Endpoint = getValue(properties, endpointKey);
        String s3Port = getValue(properties, portKey);
        if (s3Endpoint != null && s3Port != null && region != null) {
            String s3EndpointFull = "https://" + s3Endpoint + ":" + s3Port;
            builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3EndpointFull, region));
        } else {
            if (region != null) {
                builder.setRegion(region);
            }
        }
    }

    /**
     * Handle retry for amazon quotas
     *
     * @param actionLabel action label used for logging purpose only
     * @param action the action to retry
     * @param retryLimit retry number limit
     * @param durationInMillis duration between each retry
     * @param logger logger
     * @throws LimitExceededException after retry exhausted
     */
    public static void performAmazonActionWithRetry(String actionLabel, Supplier<Void> action, int retryLimit, int durationInMillis, Logger logger) {
        int retryCount = 0;
        do {
            try {
                action.get();
                return;
            } catch (LimitExceededException lee) {
                // We should just wait a little time before trying again
                logger.debug("LimitExceededException while doing " + actionLabel + " will retry " + (retryLimit - retryCount) + " times");
            }
            try {
                Thread.sleep(durationInMillis);
                logger.debug("Throttling {} for {} ms", actionLabel, durationInMillis);
            } catch (InterruptedException ignored) {
            }
        } while (retryCount++ < retryLimit);
        throw new LimitExceededException("Can't do " + actionLabel + " after " + retryLimit + " retries");
    }

}
