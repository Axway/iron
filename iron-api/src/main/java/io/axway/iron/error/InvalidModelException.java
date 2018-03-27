package io.axway.iron.error;

import java.util.function.*;
import io.axway.alf.Arguments;

/**
 * This exception is thrown when a model (entity or command) is not correctly declared.
 */
public class InvalidModelException extends StoreException {
    public InvalidModelException() {
    }

    public InvalidModelException(Throwable cause) {
        super(cause);
    }

    public InvalidModelException(String message) {
        super(message);
    }

    public InvalidModelException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidModelException(String message, Consumer<Arguments> args) {
        super(message, args);
    }

    public InvalidModelException(String message, Consumer<Arguments> args, Throwable cause) {
        super(message, args, cause);
    }
}
