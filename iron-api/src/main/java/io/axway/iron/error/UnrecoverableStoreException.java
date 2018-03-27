package io.axway.iron.error;

import java.util.function.*;
import io.axway.alf.Arguments;

/**
 * This exception is thrown when a store cannot be reopened from a snapshot.
 */
public class UnrecoverableStoreException extends StoreException {
    public UnrecoverableStoreException() {
    }

    public UnrecoverableStoreException(Throwable cause) {
        super(cause);
    }

    public UnrecoverableStoreException(String message) {
        super(message);
    }

    public UnrecoverableStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnrecoverableStoreException(String message, Consumer<Arguments> args) {
        super(message, args);
    }

    public UnrecoverableStoreException(String message, Consumer<Arguments> args, Throwable cause) {
        super(message, args, cause);
    }
}
