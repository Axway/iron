package io.axway.iron.spi.aws;

import java.util.*;

public class PropertiesHelper {

    public static final String REGION_KEY = "io.axway.iron.spi.aws.region";

    public static final String BUCKET_NAME_KEY = "io.axway.iron.spi.aws.s3.bucket_name";

    public static final String S3_ENDPOINT_KEY = "io.axway.iron.spi.aws.s3.s3_endpoint";
    public static final String S3_PORT_KEY = "io.axway.iron.spi.aws.s3.s3_port";

    public static final String KINESIS_ENDPOINT_KEY = "io.axway.iron.spi.aws.kinesis.kinesis_endpoint";
    public static final String KINESIS_PORT_KEY = "io.axway.iron.spi.aws.kinesis.kinesis_port";

    public static final String CLOUDWATCH_ENDPOINT_KEY = "io.axway.iron.spi.aws.cloudwatch.cloudwatch_endpoint";
    public static final String CLOUDWATCH_PORT_KEY = "io.axway.iron.spi.aws.cloudwatch.cloudwatch_port";

    public static final String ACCESS_KEY_KEY = "io.axway.iron.spi.aws.access_key";
    public static final String SECRET_KEY_KEY = "io.axway.iron.spi.aws.secret_key";

    public static final String DISABLE_VERIFY_CERTIFICATE_KEY = "io.axway.iron.spi.aws.disable_verify_certificate";
    public static final String DISABLE_CBOR_KEY = "io.axway.iron.spi.aws.disable_cbor";

    // Disable Cert checking to simplify testing (no need to manage certificates)
    public static final String DISABLE_CERT_CHECKING_SYSTEM_PROPERTY = "com.amazonaws.sdk.disableCertChecking";
    // Disable CBOR protocol which is not supported by kinesalite
    public static final String DISABLE_CBOR_ENV_VAR = "AWS_CBOR_DISABLE";
    public static final String DISABLE_CBOR_SYSTEM_PROPERTY = "com.amazonaws.sdk.disableCbor";

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

    public static boolean isKeySet(Properties properties, String key) {
        return properties.containsKey(key);
    }

    public static boolean checkKeyIsSet(Properties properties, String key) {
        return isKeySet(properties, key);
    }

    public static boolean manageDisableVerifyCertificate(Properties properties) {
        if (isKeySet(properties, DISABLE_VERIFY_CERTIFICATE_KEY)) {
            System.setProperty(DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "");
            return true;
        }
        return System.getProperty(DISABLE_CERT_CHECKING_SYSTEM_PROPERTY) == null;
    }

    public static void manageDisableCbor(Properties properties) {
        if (isKeySet(properties, DISABLE_CBOR_KEY)) {
            System.setProperty(DISABLE_CBOR_ENV_VAR, "");
        }
    }
}
