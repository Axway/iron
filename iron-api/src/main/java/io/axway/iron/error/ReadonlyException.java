package io.axway.iron.error;

public class ReadonlyException extends StoreException {
    public ReadonlyException(String message) {
        super(message);
    }
}
