package io.axway.iron.error;

import java.util.function.*;
import io.axway.alf.Arguments;

public class ConfigurationException extends StoreException {
    public ConfigurationException() {
    }

    public ConfigurationException(Throwable cause) {
        super(cause);
    }

    public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConfigurationException(String message, Consumer<Arguments> args) {
        super(message, args);
    }

    public ConfigurationException(String message, Consumer<Arguments> args, Throwable cause) {
        super(message, args, cause);
    }
}
