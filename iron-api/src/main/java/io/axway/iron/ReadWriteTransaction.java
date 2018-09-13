package io.axway.iron;

import java.util.*;
import javax.annotation.*;
import io.axway.iron.functional.Accessor;

/**
 * A read write transaction can be use by a {@link Command#execute(ReadWriteTransaction)} method to modify the model.
 */
public interface ReadWriteTransaction extends ReadOnlyTransaction {
    /**
     * Insert a new instance in the model.
     *
     * @param entityClass the entity class of the instance to be inserted
     * @param <E> the type corresponding to the entity class (automatically inferred)
     * @return a fluent interface to continue the call
     */
    <E> ObjectUpdater<E> insert(Class<E> entityClass);

    /**
     * Update an existing instance in the model.
     *
     * @param object the object to be update. This has been retrieved by using the methods in {@link ReadOnlyTransaction}
     * @param <E> the type corresponding to the entity class (automatically inferred)
     * @return a fluent interface to continue the call
     */
    <E> ObjectUpdater<E> update(E object);

    /**
     * Delete an existing instance in the model.
     *
     * @param object the object to be deleted. This has been retrieved by using the methods in {@link ReadOnlyTransaction}
     */
    void delete(Object object);

    interface ObjectUpdaterBase<E> {
        /**
         * Begin the update of an instance member.
         *
         * @param accessor the member accessor method reference
         * @param <V> the type of instance member
         * @return a fluent interface to continue the call
         */
        <V> To<E, V> set(Accessor<E, V> accessor);

        /**
         * Begin the update of an instance relation collection.
         *
         * @param accessor the member accessor method reference
         * @param <H> the type of instance member collection
         * @param <V> the type of instance member collection item
         * @return a fluent interface to continue the call
         */
        <H, V extends Collection<H>> CollectionUpdater<E, H> onCollection(Accessor<E, V> accessor);
    }

    interface To<E, V> {
        /**
         * Specify the value to be set for the updated member.
         *
         * @param value the new member value
         * @return a fluent interface to continue the call
         */
        ObjectUpdater<E> to(@Nullable V value);
    }

    interface ObjectUpdater<E> extends ObjectUpdaterBase<E> {
        /**
         * Must be called to validate any changes.
         *
         * @return the object instance, it is useful for {@link #insert(Class)} operations
         */
        E done();
    }

    interface CollectionUpdater<E, H> extends ObjectUpdater<E> {
        /**
         * Add one element in the relation collection.
         *
         * @param object the head entity instance to be added
         * @return the same object to continue the collection update
         */
        CollectionUpdater<E, H> add(H object);

        /**
         * Add many elements in the relation collection.
         *
         * @param objects the head entity instances to be added
         * @return the same object to continue the collection update
         */
        CollectionUpdater<E, H> addAll(Collection<H> objects);

        /**
         * Remove one element in the relation collection.
         *
         * @param object the head entity instance to be removed
         * @return the same object to continue the collection update
         */
        CollectionUpdater<E, H> remove(H object);

        /**
         * Remove many elements in the relation collection.
         *
         * @param objects the head entity instances to be removed
         * @return the same object to continue the collection update
         */
        CollectionUpdater<E, H> removeAll(Collection<H> objects);

        /**
         * Remove all elements in the relation collection.
         *
         * @return the same object to continue the collection update
         */
        CollectionUpdater<E, H> clear();
    }
}
