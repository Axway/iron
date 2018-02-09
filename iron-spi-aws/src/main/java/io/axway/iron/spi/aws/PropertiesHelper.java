package io.axway.iron.spi.aws;

import java.util.*;
import io.axway.iron.spi.aws.kinesis.AwsKinesisProperties;

public class PropertiesHelper implements AwsProperties, AwsKinesisProperties {

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
