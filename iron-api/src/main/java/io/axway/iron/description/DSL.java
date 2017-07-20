package io.axway.iron.description;

import java.util.*;
import io.axway.iron.functional.Accessor;

import static io.axway.iron.description.hook.DSLHelper.THREAD_LOCAL_DSL_HELPER;

public final class DSL {

    /**
     * This method has to be called in the default implementation of an entity interface to declare that a relation is the reciprocal of another relation:
     * <ul>
     * <li>the straight relation is defined from the {@code TAIL} entity to the {@code HEAD} entity. It must not have a default implementation</li>
     * <li>the reverse relation is defined from the {@code HEAD} entity to the {@code TAIL} entity. It must have a default implementation that call this method</li>
     * </ul>
     * <p>
     * At runtime this call is only issued during the model analysis. The entity instances proxies overrides this default implementation to return the needed information.<br>
     *
     * @param tailEntityClass the {@code TAIL} entity class
     * @param relationAccessor the accessor on the {@code TAIL} entity class that correspond to the straight relation.
     * @param <TAIL> the TAIL entity
     * @param <HEAD> the HEAD entity
     * @return whatever is needed to make the compiler happy
     */
    public static <TAIL, HEAD> Collection<TAIL> reciprocalManyRelation(Class<TAIL> tailEntityClass, Accessor<TAIL, HEAD> relationAccessor) {
        return THREAD_LOCAL_DSL_HELPER.get().reciprocalManyRelation(tailEntityClass, relationAccessor);
    }

    private DSL() {
    }
}
