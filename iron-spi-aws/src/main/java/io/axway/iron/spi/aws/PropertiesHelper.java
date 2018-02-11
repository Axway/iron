package io.axway.iron.spi.aws;

import java.util.*;
import io.axway.iron.spi.aws.kinesis.PropertyMapper;

import static java.util.Optional.*;

public class PropertiesHelper {

    public static Optional<String> getValue(Properties properties, PropertyMapper property) {
        String region = (String) properties.get(property.getPropertyKey());
        if (region != null) {
            return of(region);
        }
        region = System.getenv(property.getEnvVarName());
        if (region != null) {
            return of(region);
        }
        return empty();
    }
}
