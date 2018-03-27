package io.axway.iron.error;

import java.util.function.*;
import io.axway.alf.Arguments;

public class MalformedCommandException extends StoreException {
    public MalformedCommandException() {
    }

    public MalformedCommandException(Throwable cause) {
        super(cause);
    }

    public MalformedCommandException(String message) {
        super(message);
    }

    public MalformedCommandException(String message, Throwable cause) {
        super(message, cause);
    }

    public MalformedCommandException(String message, Consumer<Arguments> args) {
        super(message, args);
    }

    public MalformedCommandException(String message, Consumer<Arguments> args, Throwable cause) {
        super(message, args, cause);
    }
}
