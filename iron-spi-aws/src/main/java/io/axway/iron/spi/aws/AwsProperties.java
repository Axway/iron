package io.axway.iron.spi.aws;

import io.axway.iron.spi.aws.kinesis.PropertyMapper;

public enum AwsProperties implements PropertyMapper {
    REGION_KEY("io.axway.iron.spi.aws.region", "AWS_REGION"),//
    ACCESS_KEY_KEY("io.axway.iron.spi.aws.access_key", "AWS_ACCESS_KEY"),//
    SECRET_KEY_KEY("io.axway.iron.spi.aws.secret_key", "AWS_SECRET_KEY"),//
    DISABLE_VERIFY_CERTIFICATE_KEY("io.axway.iron.spi.aws.disable_verify_certificate", "DISABLE_VERIFY_CERTIFICATE");//

    private String m_propertyKey;
    private String m_envVarName;

    AwsProperties(String propertyKey, String envVarName) {
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
