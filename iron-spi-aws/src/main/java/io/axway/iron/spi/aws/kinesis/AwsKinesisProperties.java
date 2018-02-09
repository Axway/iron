package io.axway.iron.spi.aws.kinesis;

public interface AwsKinesisProperties {
    String KINESIS_ENDPOINT_KEY = "io.axway.iron.spi.aws.kinesis.kinesis_endpoint";
    String KINESIS_PORT_KEY = "io.axway.iron.spi.aws.kinesis.kinesis_port";
    String CLOUDWATCH_ENDPOINT_KEY = "io.axway.iron.spi.aws.cloudwatch.cloudwatch_endpoint";
    String CLOUDWATCH_PORT_KEY = "io.axway.iron.spi.aws.cloudwatch.cloudwatch_port";
    String DISABLE_CBOR_KEY = "io.axway.iron.spi.aws.kinesis.disable_cbor";
    // Disable CBOR protocol which is not supported by kinesalite
    String DISABLE_CBOR_ENV_VAR = "AWS_CBOR_DISABLE";
    String DISABLE_CBOR_SYSTEM_PROPERTY = "com.amazonaws.sdk.disableCbor";
}
