package io.axway.iron.spi.aws;

import java.util.function.*;
import javax.annotation.*;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import io.axway.alf.log.Logger;
import io.axway.alf.log.LoggerFactory;

public class AwsUtils {

    private static final Logger LOG = LoggerFactory.getLogger(AwsUtils.class);

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
     * @throws LimitExceededException after retry exhausted
     */
    public static void performAmazonActionWithRetry(String actionLabel, Supplier<Void> action, int retryLimit, int durationInMillis) {
        int retryCount = 0;
        do {
            try {
                action.get();
                return;
            } catch (LimitExceededException lee) {
                // We should just wait a little time before trying again
                int remainingRetries = retryLimit - retryCount;
                LOG.debug("LimitExceededException caught", args -> args.add("action", actionLabel).add("remainingRetryCount", remainingRetries));
            }
            try {
                LOG.debug("Throttling", args -> args.add("action", actionLabel).add("durationMs", durationInMillis));
                Thread.sleep(durationInMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AwsException("Thread interrupted while performing action", args -> args.add("action", actionLabel), e);
            }
        } while (retryCount++ < retryLimit);
        throw new AwsException("Limit exceeded, all retries failed", args -> args.add("action", actionLabel).add("retryLimit", retryLimit));
    }
}
