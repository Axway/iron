package io.axway.iron.spi.aws;

import java.util.*;

import static java.util.Optional.*;

public class PropertiesHelper {

    public static Optional<String> getValue(Properties properties, PropertyMapper property) {
        String value = (String) properties.get(property.getPropertyKey());
        if (value != null) {
            return of(value);
        }
        value = System.getenv(property.getEnvVarName());
        if (value != null) {
            return of(value);
        }
        return empty();
    }
}
