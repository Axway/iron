package io.axway.iron.functional;

/**
 * This interface should be only used through method reference. It's used as a workaround to support Java method literal.
 *
 * @param <E> the entity on which the method reference should be based
 * @param <V> the method reference return type
 */
@FunctionalInterface
public interface Accessor<E, V> {
    V get(E object);
}
