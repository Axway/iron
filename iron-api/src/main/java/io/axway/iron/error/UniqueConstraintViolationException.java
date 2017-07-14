package io.axway.iron.error;

import java.util.*;
import io.axway.iron.description.Unique;

/**
 * This exception is thrown when a modification operation violates a {@link Unique} constraint.
 */
public class UniqueConstraintViolationException extends StoreException {
    public UniqueConstraintViolationException(String entityName, String uniqueName, Object uniqueValue) {
        this(entityName, Collections.singletonList(uniqueName), Collections.singletonList(uniqueValue));
    }

    public UniqueConstraintViolationException(String entityName, List<String> uniqueNames, List<?> uniquesValues) {
        super("Unique constraint violation", args -> args.add("entityName", entityName).add("uniqueNames", uniqueNames).add("uniquesValues", uniquesValues));
    }
}
