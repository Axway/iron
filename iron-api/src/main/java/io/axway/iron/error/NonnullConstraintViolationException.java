package io.axway.iron.error;

import javax.annotation.*;

/**
 * This exception is thrown when a modification operation violates a {@link Nonnull} constraint.
 */
public class NonnullConstraintViolationException extends StoreException {
    public NonnullConstraintViolationException(String entityName, String fieldName) {
        super("Nonnull constraint violation", args -> args.add("entityName", entityName).add("fieldName", fieldName));
    }
}
