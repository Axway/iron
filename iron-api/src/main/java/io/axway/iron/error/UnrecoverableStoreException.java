package io.axway.iron.error;

import java.util.function.*;

/**
 * This exception is thrown when a store cannot be reopened from a snapshot.
 */
public class UnrecoverableStoreException extends StoreException {
    public UnrecoverableStoreException(String message, Consumer<Arguments> args) {
        super(message, args);
    }
}
