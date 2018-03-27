package io.axway.iron.spi.aws.kinesis;

import java.util.function.*;
import io.axway.alf.Arguments;
import io.axway.iron.spi.aws.AwsException;

public class AwsKinesisException extends AwsException {
    public AwsKinesisException() {
    }

    public AwsKinesisException(Throwable cause) {
        super(cause);
    }

    public AwsKinesisException(String message) {
        super(message);
    }

    public AwsKinesisException(String message, Throwable cause) {
        super(message, cause);
    }

    public AwsKinesisException(String message, Consumer<Arguments> args) {
        super(message, args);
    }

    public AwsKinesisException(String message, Consumer<Arguments> args, Throwable cause) {
        super(message, args, cause);
    }
}
