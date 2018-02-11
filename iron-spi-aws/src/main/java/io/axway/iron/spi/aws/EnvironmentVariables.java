package io.axway.iron.spi.aws;

public interface EnvironmentVariables {

    // Disable Cert checking to simplify testing (no need to manage certificates)
    //DISABLE_CERT_CHECKING_SYSTEM_PROPERTY("com.amazonaws.sdk.disableCertChecking");//

    //// Disable CBOR protocol which is not supported by kinesalite
    String DISABLE_CBOR_ENV_VAR = "AWS_CBOR_DISABLE";
    //String DISABLE_CBOR_SYSTEM_PROPERTY = "com.amazonaws.sdk.disableCbor";
}
