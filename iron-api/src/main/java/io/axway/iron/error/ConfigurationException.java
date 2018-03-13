package io.axway.iron.error;

import java.util.function.*;

public class ConfigurationException extends StoreException {

    public ConfigurationException() {
    }

    public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(Throwable cause) {
        super(cause);
    }

    public ConfigurationException(Throwable cause, String message) {
        super(cause, message);
    }

    public ConfigurationException(String message, Consumer<Arguments> args) {
        super(message, args);
    }

    public ConfigurationException(String message, Consumer<Arguments> args, Throwable cause) {
        super(message, args, cause);
    }
}
