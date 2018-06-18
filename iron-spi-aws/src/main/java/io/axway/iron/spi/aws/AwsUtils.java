package io.axway.iron.spi.aws;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import javax.annotation.*;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.model.KMSThrottlingException;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import io.axway.alf.log.Logger;
import io.axway.alf.log.LoggerFactory;

import static java.util.Optional.empty;
import static java.util.Optional.of;

public final class AwsUtils {

    private static final Logger LOG = LoggerFactory.getLogger(AwsUtils.class);

    private static final Random RANDOM = new Random();

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
     * @throws AwsException after retry exhausted
     */
    public static <T> T performAmazonActionWithRetry(String actionLabel, Supplier<T> action, int retryLimit, int durationInMillis) {
        int retryCount = 0;
        do {
            try {
                return action.get();
            } catch (LimitExceededException | ProvisionedThroughputExceededException | KMSThrottlingException e) {
                // We should just wait a little time before trying again
                int remainingRetries = retryLimit - retryCount;
                LOG.debug("Amazon exception caught",
                          args -> args.add("exception", e.getClass().getName()).add("action", actionLabel).add("remainingRetryCount", remainingRetries));
            }
            sleepUntilInterrupted(actionLabel, durationInMillis);
        } while (retryCount++ < retryLimit);
        throw new AwsException("Limit exceeded, all retries failed", args -> args.add("action", actionLabel).add("retryLimit", retryLimit));
    }

    /**
     * Try to perform an Amazon action and increase the duration between requests if some exception is exceeding resource usage exception is thrown.
     *
     * @param actionLabel action label used for logging purpose only
     * @param action the action to retry
     * @param durationBetweenRequests duration between each retry
     */
    public static <T> Optional<T> tryAmazonAction(String actionLabel, Supplier<T> action, AtomicLong durationBetweenRequests) {
        try {
            return of(action.get());
        } catch (LimitExceededException | ProvisionedThroughputExceededException | KMSThrottlingException e) {
            int durationRandomModifier = 1 + RANDOM.nextInt(64);// random duration to make readers out of sync, avoiding simultaneous readings
            long updatedDuration = durationBetweenRequests.updateAndGet(duration -> duration * 2 // twice the duration
                    + duration * 2 / durationRandomModifier);// add random duration to avoid simultaneous reads
            LOG.debug("Update of minimal duration between two get shard iterator requests",
                      args -> args.add("actionLabel", actionLabel).add("new minimalDurationBetweenTwoGetShardIteratorRequests", updatedDuration));
        }
        return empty();
    }

    public static void sleepUntilInterrupted(String actionLabel, int durationInMillis) {
        try {
            LOG.debug("Throttling", args -> args.add("action", actionLabel).add("durationMs", durationInMillis));
            Thread.sleep(durationInMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AwsException("Thread interrupted while performing action", args -> args.add("action", actionLabel), e);
        }
    }

    private AwsUtils() {
        // utility class
    }
}
