package io.axway.iron.error;

import java.util.function.*;
import io.axway.alf.Arguments;
import io.axway.alf.exception.FormattedRuntimeException;

/**
 * The root exception for all exception related to store.
 */
public class StoreException extends FormattedRuntimeException {
    public StoreException() {
    }

    public StoreException(Throwable cause) {
        super(cause);
    }

    public StoreException(String message) {
        super(message);
    }

    public StoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public StoreException(String message, Consumer<Arguments> args) {
        super(message, args);
    }

    public StoreException(String message, Consumer<Arguments> args, Throwable cause) {
        super(message, args, cause);
    }
}
