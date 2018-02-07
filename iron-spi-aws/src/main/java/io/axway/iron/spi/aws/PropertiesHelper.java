package io.axway.iron.spi.aws;

import java.util.*;

public class PropertiesHelper {

    public static final String BUCKET_NAME_KEY = "io.axway.iron.spi.aws.s3.bucket_name";
    public static final String S3_ENDPOINT_KEY = "io.axway.iron.spi.aws.s3.s3_endpoint";
    public static final String S3_PORT_KEY = "io.axway.iron.spi.aws.s3.s3_port";
    public static final String ACCESS_KEY_KEY = "io.axway.iron.spi.aws.access_key";
    public static final String SECRET_KEY_KEY = "io.axway.iron.spi.aws.secret_key";
    public static final String REGION_KEY = "io.axway.iron.spi.aws.region";
    // Disable Cert checking to simplify testing (no need to manage certificates)
    public static final String DISABLE_CERT_CHECKING_ENV_VAR = "com.amazonaws.sdk.disableCertChecking";
    // Disable CBOR protocol which is not supported by kinesalite
    public static final String DISABLE_CBOR_ENV_VAR = "com.amazonaws.sdk.disableCertChecking";

    public static String checkKeyHasValue(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value != null) {
            String trimmedValue = value.trim();
            if (!trimmedValue.isEmpty()) {
                return trimmedValue;
            }
        }
        throw new IllegalArgumentException("Property with key " + key + " is not set or has an empty value");
    }

    public static Long checkKeyHasLongValue(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value != null) {
            String trimmedValue = value.trim();
            if (!trimmedValue.isEmpty()) {
                Long valueAsLong = null;
                try {
                    valueAsLong = Long.valueOf(trimmedValue);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Property with key " + key + " = " + trimmedValue + " is not a long");
                }
                return valueAsLong;
            }
        }
        throw new IllegalArgumentException("Property with key " + key + " is not set or has an empty value");
    }

    public static boolean checkKeyIsSet(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value != null) {
            return true;
        }
        throw new IllegalArgumentException("Property with key " + key + " is not set");
    }
}
