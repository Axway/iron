package io.axway.iron;

import java.util.*;
import javax.annotation.*;
import io.axway.iron.error.ObjectNotFoundException;
import io.axway.iron.functional.Accessor;

/**
 * The read only transaction permit to read the store model through the {{@link #select(Class)} method.<br>
 * Many read only transaction can be performed simultaneously but no command execution is performed at the same.
 */
public interface ReadOnlyTransaction {
    /**
     * @param entityClass the entity class of the instance(s) to be loaded
     * @param <E> the type corresponding to the entity class (automatically inferred)
     * @return a fluent interface to continue the call
     */
    <E> From<E> select(Class<E> entityClass);

    interface From<E> {
        /**
         * Load all the instances of an entity.
         *
         * @return a collection that contains all the instances of the selected entity
         */
        Collection<E> all();

        /**
         * Permit to lookup some specific instance based on a unique constraint.
         *
         * @param accessor the accessor that correspond to the unique constraint member
         * @param <V> the type of the unique constraint
         * @return a fluent interface to continue the call
         */
        <V> On<E, V> where(Accessor<E, V> accessor);
    }

    interface On<E, V> {
        /**
         * Load exactly one instance of the selected entity.
         *
         * @param value a single value to be looked for
         * @return the instance that match the constraint
         * @throws ObjectNotFoundException in case no instance fulfilling the constraint has been found
         */
        E equalsTo(V value);

        /**
         * Load one instance of the selected entity if exists.
         *
         * @param value a single value to be looked for
         * @return the instance that match the constraint or {@code null} if no instance is matching
         */
        @Nullable
        E equalsToOrNull(V value);

        /**
         * Load many instances of the selected entity.
         *
         * @param values values to be looked for
         * @return a collection that contains matched instances. The matched instances are in the same order than the {@code values}, so the returned collection has the same size than {@code values}
         * @throws ObjectNotFoundException in case no instance fulfilling one of the values
         */
        Collection<E> allContainedIn(Collection<V> values);

        /**
         * Load many instances of the selected entity.
         *
         * @param values values to be looked for
         * @return a collection that contains matched instances. The matched instances are in the same order than the {@code values}, in case a value is not matched it doesn't add an entry in the result. So the returned collection has size equals or smaller than {@code values}.
         */
        Collection<E> someContainedIn(Collection<V> values);
    }
}
