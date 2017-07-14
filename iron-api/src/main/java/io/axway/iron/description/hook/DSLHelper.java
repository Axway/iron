package io.axway.iron.description.hook;

import java.util.*;
import io.axway.iron.functional.Accessor;

/**
 * This is an internal interface that must not be directly used.
 */
public interface DSLHelper {
    ThreadLocal<DSLHelper> THREAD_LOCAL_DSL_HELPER = new ThreadLocal<>();

    <TAIL, HEAD> Collection<TAIL> reciprocalManyRelation(Class<TAIL> tailEntityClass, Accessor<TAIL, HEAD> relationAccessor);
}
