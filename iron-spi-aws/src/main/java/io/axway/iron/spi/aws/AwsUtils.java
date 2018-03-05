package io.axway.iron.spi.aws;

import java.util.*;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;

import static io.axway.iron.spi.aws.AwsProperties.*;
import static io.axway.iron.spi.aws.PropertiesHelper.getValue;

public class AwsUtils {

    public static void setAws(Properties properties, AwsClientBuilder builder, PropertyMapper endpointKey, PropertyMapper portKey) {
        Optional<String> accessKey = getValue(properties, ACCESS_KEY_KEY);
        Optional<String> secretKey = getValue(properties, SECRET_KEY_KEY);
        if (accessKey.isPresent() && secretKey.isPresent()) {
            builder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey.get(), secretKey.get())));
        }
        Optional<String> region = getValue(properties, REGION_KEY);
        Optional<String> s3Endpoint = getValue(properties, endpointKey);
        Optional<String> s3Port = getValue(properties, portKey);
        if (s3Endpoint.isPresent() && s3Port.isPresent() && region.isPresent()) {
            String s3EndpointFull = "https://" + s3Endpoint.get() + ":" + s3Port.get();
            builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3EndpointFull, region.get()));
        } else {
            region.ifPresent(builder::setRegion);
        }
    }
}
