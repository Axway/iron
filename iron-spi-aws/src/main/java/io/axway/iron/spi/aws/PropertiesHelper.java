package io.axway.iron.spi.aws;

import java.util.*;
import javax.annotation.*;

public class PropertiesHelper {

    @Nullable
    public static String getValue(Properties properties, AwsProperties property) {
        String value = (String) properties.get(property.getPropertyKey());
        if (value != null) {
            return value;
        }
        value = System.getenv(property.getEnvVarName());
        if (value != null) {
            return value;
        }
        return null;
    }

    public static boolean isSet(Properties properties, AwsProperties property) {
        String value = PropertiesHelper.getValue(properties, property);
        return value != null && (value.equals("") || value.equalsIgnoreCase("true"));
    }
}
