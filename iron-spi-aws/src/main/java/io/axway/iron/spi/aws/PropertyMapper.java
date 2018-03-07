package io.axway.iron.spi.aws;

/**
 * Maps properties between property file and environment variable
 */
public interface PropertyMapper {
    String getPropertyKey();

    String getEnvVarName();
}
