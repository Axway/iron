package io.axway.iron.spi.aws;

import java.util.function.*;
import javax.annotation.*;
import org.slf4j.Logger;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.model.LimitExceededException;

public class AwsUtils {

    public static void setAws(AwsClientBuilder builder, //
                              @Nullable String accessKey, @Nullable String secretKey, //
                              @Nullable String endpoint, @Nullable Integer port, @Nullable String region) {
        if (accessKey != null && secretKey != null) {
            builder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)));
        }
        if (region != null) {
            if (endpoint != null && port != null) {
                String s3EndpointFull = "https://" + endpoint + ":" + port;
                builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3EndpointFull, region));
            } else {
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
