package io.axway.iron.spi.aws;

import static io.axway.iron.spi.aws.AwsProperties.Constants.*;

public enum AwsProperties implements PropertyMapper {

    REGION_KEY(AWS_REGION_PROPERTY, AWS_REGION_ENVVAR),//
    ACCESS_KEY_KEY(AWS_ACCESS_KEY_PROPERTY, AWS_ACCESS_KEY_ENVVAR),//
    SECRET_KEY_KEY(AWS_SECRET_KEY_PROPERTY, AWS_SECRET_KEY_ENVVAR),//
    DISABLE_VERIFY_CERTIFICATE_KEY(AWS_DISABLE_VERIFY_CERTIFICATE_PROPERTY, AWS_DISABLE_VERIFY_CERTIFICATE_ENVVAR);

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

    /**
     * Set as constants for documentation purpose only.
     */
    public static class Constants {
        public static final String AWS_REGION_PROPERTY = "io.axway.iron.spi.aws.region";
        public static final String AWS_REGION_ENVVAR = "AWS_REGION";
        public static final String AWS_ACCESS_KEY_PROPERTY = "io.axway.iron.spi.aws.access_key";
        public static final String AWS_ACCESS_KEY_ENVVAR = "AWS_ACCESS_KEY";
        public static final String AWS_SECRET_KEY_PROPERTY = "io.axway.iron.spi.aws.secret_key";
        public static final String AWS_SECRET_KEY_ENVVAR = "AWS_SECRET_KEY";
        public static final String AWS_DISABLE_VERIFY_CERTIFICATE_PROPERTY = "io.axway.iron.spi.aws.disable_verify_certificate";
        public static final String AWS_DISABLE_VERIFY_CERTIFICATE_ENVVAR = "DISABLE_VERIFY_CERTIFICATE";
    }
}
