package io.axway.iron.error;

import java.util.function.*;

/**
 * This exception is thrown when a model (entity or command) is not correctly declared.
 */
public class InvalidModelException extends StoreException {
    public InvalidModelException(String message, Consumer<Arguments> args) {
        super(message, args);
    }
}
