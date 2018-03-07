package io.axway.iron.spi.aws.s3;

import java.util.function.*;
import io.axway.iron.error.StoreException;

public class AwsS3Exception extends StoreException {

    public AwsS3Exception() {
    }

    public AwsS3Exception(String message) {
        super(message);
    }

    public AwsS3Exception(Throwable cause) {
        super(cause);
    }

    public AwsS3Exception(Throwable cause, String message) {
        super(cause, message);
    }

    public AwsS3Exception(String message, Consumer<Arguments> args) {
        super(message, args);
    }

    public AwsS3Exception(String message, Consumer<Arguments> args, Throwable cause) {
        super(message, args, cause);
    }
}
