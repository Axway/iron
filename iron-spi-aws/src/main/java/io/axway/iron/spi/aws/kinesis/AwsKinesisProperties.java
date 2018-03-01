package io.axway.iron.spi.aws.kinesis;

import io.axway.iron.spi.aws.PropertyMapper;

/**
 * Kinesis can be configured using Properties or Environment Variables.
 */
public enum AwsKinesisProperties implements PropertyMapper {
    KINESIS_ENDPOINT_KEY(Constants.AWS_KINESIS_ENDPOINT_PROPERTY, Constants.AWS_KINESIS_ENDPOINT_ENVVAR),//
    KINESIS_PORT_KEY(Constants.AWS_KINESIS_PORT_PROPERTY, Constants.AWS_KINESIS_PORT_ENVVAR),//
    CLOUDWATCH_ENDPOINT_KEY(Constants.AWS_CLOUDWATCH_ENDPOINT_PROPERTY, Constants.AWS_CLOUDWATCH_ENDPOINT_ENVVAR),//
    CLOUDWATCH_PORT_KEY(Constants.AWS_CLOUDWATCH_PORT_PROPERTY, Constants.AWS_CLOUDWATCH_PORT_ENVVAR);

    private String m_propertyKey;
    private String m_envVarName;

    AwsKinesisProperties(String propertyKey, String envVarName) {
        m_propertyKey = propertyKey;
        m_envVarName = envVarName;
    }

    @Override
    public String getPropertyKey() {
        return m_propertyKey;
    }

    @Override
    public String getEnvVarName() {
        return m_envVarName;
    }

    /**
     * Set as constants for documentation purpose only.
     */
    static class Constants {
        static final String AWS_KINESIS_ENDPOINT_PROPERTY = "io.axway.iron.spi.aws.kinesis.kinesis_endpoint";
        static final String AWS_KINESIS_ENDPOINT_ENVVAR = "AWS_KINESIS_ENDPOINT";
        static final String AWS_KINESIS_PORT_PROPERTY = "io.axway.iron.spi.aws.kinesis.kinesis_port";
        static final String AWS_KINESIS_PORT_ENVVAR = "AWS_KINESIS_PORT";
        static final String AWS_CLOUDWATCH_ENDPOINT_PROPERTY = "io.axway.iron.spi.aws.cloudwatch.cloudwatch_endpoint";
        static final String AWS_CLOUDWATCH_ENDPOINT_ENVVAR = "AWS_CLOUDWATCH_ENDPOINT";
        static final String AWS_CLOUDWATCH_PORT_PROPERTY = "io.axway.iron.spi.aws.cloudwatch.cloudwatch_port";
        static final String AWS_CLOUDWATCH_PORT_ENVVAR = "AWS_CLOUDWATCH_PORT";
    }
}
