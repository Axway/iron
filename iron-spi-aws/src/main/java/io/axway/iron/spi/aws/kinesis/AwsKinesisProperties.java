package io.axway.iron.spi.aws.kinesis;

import io.axway.iron.spi.aws.PropertyMapper;

public enum AwsKinesisProperties implements PropertyMapper {
    KINESIS_ENDPOINT_KEY("io.axway.iron.spi.aws.kinesis.kinesis_endpoint", "AWS_KINESIS_ENDPOINT"),//
    KINESIS_PORT_KEY("io.axway.iron.spi.aws.kinesis.kinesis_port", "AWS_KINESIS_PORT"),//
    CLOUDWATCH_ENDPOINT_KEY("io.axway.iron.spi.aws.cloudwatch.cloudwatch_endpoint", "AWS_CLOUDWATCH_ENDPOINT"),//
    CLOUDWATCH_PORT_KEY("io.axway.iron.spi.aws.cloudwatch.cloudwatch_port", "AWS_CLOUDWATCH_PORT");

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
}
