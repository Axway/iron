package io.axway.iron.spi.aws;

public class AwsTestUtils {

    public static String getValue(String key, String defaultValue) {
        String envValue = System.getenv().containsKey(key) ? System.getenv().get(key) : null;
        return envValue != null ? envValue : defaultValue;
    }
}
