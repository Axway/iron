package io.axway.iron.spi.aws;

public interface AwsProperties {
    String REGION_KEY = "io.axway.iron.spi.aws.region";
    String ACCESS_KEY_KEY = "io.axway.iron.spi.aws.access_key";
    String SECRET_KEY_KEY = "io.axway.iron.spi.aws.secret_key";
    String DISABLE_VERIFY_CERTIFICATE_KEY = "io.axway.iron.spi.aws.disable_verify_certificate";
    // Disable Cert checking to simplify testing (no need to manage certificates)
    String DISABLE_CERT_CHECKING_SYSTEM_PROPERTY = "com.amazonaws.sdk.disableCertChecking";
}
