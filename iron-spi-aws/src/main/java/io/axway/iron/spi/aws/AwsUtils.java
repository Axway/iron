package io.axway.iron.spi.aws;

import java.util.*;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;

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
}
