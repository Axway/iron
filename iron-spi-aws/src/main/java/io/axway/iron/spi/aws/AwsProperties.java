package io.axway.iron.spi.aws;

import static io.axway.iron.spi.aws.AwsProperties.Constants.*;

public enum AwsProperties implements PropertyMapper {
    // AWS GLOBAL
    REGION_KEY(AWS_REGION_PROPERTY, AWS_REGION_ENVVAR),//
    ACCESS_KEY_KEY(AWS_ACCESS_KEY_PROPERTY, AWS_ACCESS_KEY_ENVVAR),//
    SECRET_KEY_KEY(AWS_SECRET_KEY_PROPERTY, AWS_SECRET_KEY_ENVVAR),//
    DISABLE_VERIFY_CERTIFICATE_KEY(AWS_DISABLE_VERIFY_CERTIFICATE_PROPERTY, AWS_DISABLE_VERIFY_CERTIFICATE_ENVVAR),//
    // AWS KINESIS
    KINESIS_ENDPOINT_KEY(AWS_KINESIS_ENDPOINT_PROPERTY, AWS_KINESIS_ENDPOINT_ENVVAR),//
    KINESIS_PORT_KEY(AWS_KINESIS_PORT_PROPERTY, AWS_KINESIS_PORT_ENVVAR),//
    CLOUDWATCH_ENDPOINT_KEY(AWS_CLOUDWATCH_ENDPOINT_PROPERTY, AWS_CLOUDWATCH_ENDPOINT_ENVVAR),//
    CLOUDWATCH_PORT_KEY(AWS_CLOUDWATCH_PORT_PROPERTY, AWS_CLOUDWATCH_PORT_ENVVAR),//
    KINESIS_STREAM_NAME_PREFIX(AWS_KINESIS_STREAM_NAME_PREFIX_PROPERTY, AWS_KINESIS_STREAM_NAME_PREFIX_ENVVAR),//
    // AWS S3
    DISABLE_CBOR_KEY(AWS_DISABLE_CBOR_PROPERTY, AWS_CBOR_DISABLE_ENVVAR),//
    S3_ENDPOINT_KEY(AWS_S3_ENDPOINT_PROPERTY, AWS_S3_ENDPOINT_ENVVAR),//
    S3_PORT_KEY(AWS_S3_PORT_PROPERTY, AWS_S3_PORT_ENVVAR),//
    S3_BUCKET_NAME_KEY(AWS_S3_BUCKET_NAME_PROPERTY, AWS_S3_BUCKET_NAME_ENVVAR);

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
        // AWS GLOBAL
        public static final String AWS_REGION_PROPERTY = "io.axway.iron.spi.aws.region";
        public static final String AWS_REGION_ENVVAR = "AWS_REGION";
        public static final String AWS_ACCESS_KEY_PROPERTY = "io.axway.iron.spi.aws.access_key";
        public static final String AWS_ACCESS_KEY_ENVVAR = "AWS_ACCESS_KEY";
        public static final String AWS_SECRET_KEY_PROPERTY = "io.axway.iron.spi.aws.secret_key";
        public static final String AWS_SECRET_KEY_ENVVAR = "AWS_SECRET_KEY";
        public static final String AWS_DISABLE_VERIFY_CERTIFICATE_PROPERTY = "io.axway.iron.spi.aws.disable_verify_certificate";
        public static final String AWS_DISABLE_VERIFY_CERTIFICATE_ENVVAR = "DISABLE_VERIFY_CERTIFICATE";
        // AWS KINESIS
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
        public static final String AWS_KINESIS_STREAM_NAME_PREFIX_PROPERTY = "io.axway.iron.spi.aws.kinesis.kinesis_stream_name_prefix";
        public static final String AWS_KINESIS_STREAM_NAME_PREFIX_ENVVAR = "AWS_KINESIS_STREAM_NAME_PREFIX";
        // AWS S3
        public static final String AWS_S3_ENDPOINT_PROPERTY = "io.axway.iron.spi.aws.s3.s3_endpoint";
        public static final String AWS_S3_ENDPOINT_ENVVAR = "AWS_S3_ENDPOINT";
        public static final String AWS_S3_PORT_PROPERTY = "io.axway.iron.spi.aws.s3.s3_port";
        public static final String AWS_S3_PORT_ENVVAR = "AWS_S3_PORT";
        public static final String AWS_S3_BUCKET_NAME_PROPERTY = "io.axway.iron.spi.aws.s3.bucket_name";
        public static final String AWS_S3_BUCKET_NAME_ENVVAR = "AWS_S3_BUCKET_NAME";
    }
}
