package io.axway.iron.spi.aws;

import java.util.function.*;
import io.axway.alf.Arguments;
import io.axway.iron.error.StoreException;

public class AwsException extends StoreException {
    public AwsException() {
    }

    public AwsException(Throwable cause) {
        super(cause);
    }

    public AwsException(String message) {
        super(message);
    }

    public AwsException(String message, Throwable cause) {
        super(message, cause);
    }

    public AwsException(String message, Consumer<Arguments> args) {
        super(message, args);
    }

    public AwsException(String message, Consumer<Arguments> args, Throwable cause) {
        super(message, args, cause);
    }
}
