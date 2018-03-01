package io.axway.iron.spi.aws.s3;

import io.axway.iron.spi.aws.PropertyMapper;

public enum AwsS3Properties implements PropertyMapper {
    S3_ENDPOINT_KEY(Constants.AWS_S3_ENDPOINT_PROPERTY, Constants.AWS_S3_ENDPOINT_ENVVAR),//
    S3_PORT_KEY(Constants.AWS_S3_PORT_PROPERTY, Constants.AWS_S3_PORT_ENVVAR),//
    S3_BUCKET_NAME_KEY(Constants.AWS_S3_BUCKET_NAME_PROPERTY, Constants.AWS_S3_BUCKET_NAME_ENVVAR);

    private String m_propertyKey;
    private String m_envVarName;

    AwsS3Properties(String propertyKey, String envVarName) {
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

    private static class Constants {
        static final String AWS_S3_ENDPOINT_PROPERTY = "io.axway.iron.spi.aws.s3.s3_endpoint";
        static final String AWS_S3_ENDPOINT_ENVVAR = "AWS_S3_ENDPOINT";
        static final String AWS_S3_PORT_PROPERTY = "io.axway.iron.spi.aws.s3.s3_port";
        static final String AWS_S3_PORT_ENVVAR = "AWS_S3_PORT";
        static final String AWS_S3_BUCKET_NAME_PROPERTY = "io.axway.iron.spi.aws.s3.bucket_name";
        static final String AWS_S3_BUCKET_NAME_ENVVAR = "AWS_S3_BUCKET_NAME";
    }
}
