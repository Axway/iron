package io.axway.iron.spi.aws.kinesis;

import io.axway.iron.spi.aws.PropertyMapper;

import static io.axway.iron.spi.aws.kinesis.AwsKinesisProperties.Constants.*;

/**
 * Kinesis can be configured using Properties or Environment Variables.
 */
public enum AwsKinesisProperties implements PropertyMapper {
    KINESIS_ENDPOINT_KEY(AWS_KINESIS_ENDPOINT_PROPERTY, AWS_KINESIS_ENDPOINT_ENVVAR),//
    KINESIS_PORT_KEY(AWS_KINESIS_PORT_PROPERTY, AWS_KINESIS_PORT_ENVVAR),//
    CLOUDWATCH_ENDPOINT_KEY(AWS_CLOUDWATCH_ENDPOINT_PROPERTY, AWS_CLOUDWATCH_ENDPOINT_ENVVAR),//
    CLOUDWATCH_PORT_KEY(AWS_CLOUDWATCH_PORT_PROPERTY, AWS_CLOUDWATCH_PORT_ENVVAR),//
    DISABLE_CBOR_KEY(AWS_DISABLE_CBOR_PROPERTY, AWS_CBOR_DISABLE_ENVVAR);

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
        public static final String AWS_KINESIS_ENDPOINT_PROPERTY = "io.axway.iron.spi.aws.kinesis.kinesis_endpoint";
        public static final String AWS_KINESIS_ENDPOINT_ENVVAR = "AWS_KINESIS_ENDPOINT";
        public static final String AWS_KINESIS_PORT_PROPERTY = "io.axway.iron.spi.aws.kinesis.kinesis_port";
        public static final String AWS_KINESIS_PORT_ENVVAR = "AWS_KINESIS_PORT";
        public static final String AWS_CLOUDWATCH_ENDPOINT_PROPERTY = "io.axway.iron.spi.aws.cloudwatch.cloudwatch_endpoint";
        public static final String AWS_CLOUDWATCH_ENDPOINT_ENVVAR = "AWS_CLOUDWATCH_ENDPOINT";
        public static final String AWS_CLOUDWATCH_PORT_PROPERTY = "io.axway.iron.spi.aws.cloudwatch.cloudwatch_port";
        public static final String AWS_CLOUDWATCH_PORT_ENVVAR = "AWS_CLOUDWATCH_PORT";
        public static final String AWS_DISABLE_CBOR_PROPERTY = "io.axway.iron.spi.aws.kinesis.disable_cbor";
        public static final String AWS_CBOR_DISABLE_ENVVAR = "AWS_CBOR_DISABLE";
    }
}
