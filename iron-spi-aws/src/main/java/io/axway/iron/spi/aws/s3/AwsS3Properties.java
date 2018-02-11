package io.axway.iron.spi.aws.s3;

import io.axway.iron.spi.aws.kinesis.PropertyMapper;

public enum AwsS3Properties implements PropertyMapper {
    BUCKET_NAME_KEY("io.axway.iron.spi.aws.s3.bucket_name", "AWS_S3_BUCKET_NAME"),//
    S3_ENDPOINT_KEY("io.axway.iron.spi.aws.s3.s3_endpoint", "AWS_S3_ENDPOINT"),//
    S3_PORT_KEY("io.axway.iron.spi.aws.s3.s3_port", "AWS_S3_PORT");//

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
}
