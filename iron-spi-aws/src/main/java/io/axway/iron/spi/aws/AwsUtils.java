package io.axway.iron.spi.aws;

import javax.annotation.*;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;

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
}
