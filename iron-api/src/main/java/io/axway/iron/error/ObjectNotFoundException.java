package io.axway.iron.error;

import java.util.*;

/**
 * This exception is thrown when a mandatory select operation didn't found the required entity instance.
 */
public class ObjectNotFoundException extends StoreException {
    public ObjectNotFoundException(String entityName, String uniqueName, Object uniqueValue) {
        this(entityName, Collections.singletonList(uniqueName), Collections.singletonList(uniqueValue));
    }

    public ObjectNotFoundException(String entityName, List<String> uniqueNames, List<?> uniquesValues) {
        super("Object not found", args -> args.add("entityName", entityName).add("uniqueNames", uniqueNames).add("uniqueValues", uniquesValues));
    }
}
