package io.axway.iron.spi.aws.kinesis;

import java.util.function.*;
import io.axway.iron.error.StoreException;

public class AwsKinesisException extends StoreException {

    public AwsKinesisException() {
    }

    public AwsKinesisException(String message) {
        super(message);
    }

    public AwsKinesisException(Throwable cause) {
        super(cause);
    }

    public AwsKinesisException(Throwable cause, String message) {
        super(cause, message);
    }

    public AwsKinesisException(String message, Consumer<Arguments> args) {
        super(message, args);
    }

    public AwsKinesisException(String message, Consumer<Arguments> args, Throwable cause) {
        super(message, args, cause);
    }
}
