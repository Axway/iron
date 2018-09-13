package io.axway.iron.core.internal.transaction;

import java.util.*;
import java.util.concurrent.atomic.*;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.core.internal.entity.EntityStore;
import io.axway.iron.core.internal.entity.EntityStores;
import io.axway.iron.core.internal.utils.IntrospectionHelper;
import io.axway.iron.error.StoreException;
import io.axway.iron.functional.Accessor;

import static io.axway.alf.assertion.Assertion.checkState;

public class ReadWriteTransactionImpl extends ReadOnlyTransactionImpl implements ReadWriteTransaction {
    private final List<Runnable> m_rollbackActions = new ArrayList<>();
    private final AtomicInteger m_activeObjectUpdaterCount = new AtomicInteger();

    public ReadWriteTransactionImpl(IntrospectionHelper introspectionHelper, EntityStores entityStores) {
        super(introspectionHelper, entityStores);
    }

    public int getActiveObjectUpdaterCount() {
        return m_activeObjectUpdaterCount.get();
    }

    public void rollback() {
        int i = m_rollbackActions.size();
        while (i > 0) {
            i--;
            m_rollbackActions.get(i).run();
        }
    }

    @Override
    public <E> ObjectUpdater<E> insert(Class<E> entityClass) {
        EntityStore<E> entityStore = m_entityStores.getEntityStore(entityClass);
        E object = entityStore.newInstance();
        return new InsertObjectUpdater<>(entityStore, object);
    }

    @Override
    public <E> ObjectUpdater<E> update(E object) {
        EntityStore<E> entityStore = m_entityStores.getEntityStoreByObject(object);
        return new UpdateObjectUpdater<>(entityStore, object);
    }

    @Override
    public void delete(Object object) {
        EntityStore<Object> entityStore = m_entityStores.getEntityStoreByObject(object);
        Runnable rollback = entityStore.delete(object);
        m_rollbackActions.add(rollback);
    }

    private class InsertObjectUpdater<E> implements ObjectUpdater<E> {
        private final EntityStore<E> m_entityStore;
        private final E m_object;
        private boolean m_valid = true;

        private InsertObjectUpdater(EntityStore<E> entityStore, E object) {
            m_activeObjectUpdaterCount.incrementAndGet();
            m_entityStore = entityStore;
            m_object = object;
        }

        @Override
        public <V> To<E, V> set(Accessor<E, V> accessor) {
            checkValid();
            return value -> {
                checkValid();
                String propertyName = m_introspectionHelper.getMethodName(m_entityStore.getEntityDefinition().getEntityClass(), accessor);
                m_entityStore.set(m_object, propertyName, value);
                return InsertObjectUpdater.this;
            };
        }

        @Override
        public <H, V extends Collection<H>> CollectionUpdater<E, H> onCollection(Accessor<E, V> accessor) {
            checkValid();
            throw new UnsupportedOperationException("Not yet implemented"); // TODO implements multiple relation
        }

        @Override
        public E done() {
            checkValid();
            m_valid = false;
            Runnable rollback = m_entityStore.insert(m_object);
            m_rollbackActions.add(rollback);
            m_activeObjectUpdaterCount.decrementAndGet();
            return m_object;
        }

        private void checkValid() {
            checkState(m_valid, "This InsertObjectUpdater is no more usable");
        }
    }

    private class UpdateObjectUpdater<E> implements ObjectUpdater<E> {
        private final EntityStore<E> m_entityStore;
        private final E m_object;
        private boolean m_valid = true;

        private UpdateObjectUpdater(EntityStore<E> entityStore, E object) {
            m_activeObjectUpdaterCount.incrementAndGet();
            m_entityStore = entityStore;
            m_object = object;
        }

        @Override
        public <V> To<E, V> set(Accessor<E, V> accessor) {
            checkValid();
            return value -> {
                checkValid();
                Runnable rollback = m_entityStore.update(m_object, getPropertyName(accessor), value);
                m_rollbackActions.add(rollback);
                return UpdateObjectUpdater.this;
            };
        }

        @Override
        public <H, V extends Collection<H>> CollectionUpdater<E, H> onCollection(Accessor<E, V> accessor) {
            checkValid();

            return new CollectionUpdater<E, H>() {
                @Override
                public E done() {
                    return UpdateObjectUpdater.this.done();
                }

                @Override
                public <V2> To<E, V2> set(Accessor<E, V2> accessor) {
                    return UpdateObjectUpdater.this.set(accessor);
                }

                @Override
                public <H2, V2 extends Collection<H2>> CollectionUpdater<E, H2> onCollection(Accessor<E, V2> accessor) {
                    return UpdateObjectUpdater.this.onCollection(accessor);
                }

                @Override
                public CollectionUpdater<E, H> add(H object) {
                    checkValid();
                    Runnable rollback = m_entityStore.updateCollectionAdd(m_object, getPropertyName(accessor), object);
                    m_rollbackActions.add(rollback);
                    return this;
                }

                @Override
                public CollectionUpdater<E, H> addAll(Collection<H> objects) {
                    checkValid();
                    if (objects.stream().anyMatch(Objects::isNull)) {
                        throw new StoreException("null values are not authorized for relations");
                    }
                    Runnable rollback = m_entityStore.updateCollectionAddAll(m_object, getPropertyName(accessor), objects);
                    m_rollbackActions.add(rollback);
                    return this;
                }

                @Override
                public CollectionUpdater<E, H> remove(H object) {
                    checkValid();
                    Runnable rollback = m_entityStore.updateCollectionRemove(m_object, getPropertyName(accessor), object);
                    m_rollbackActions.add(rollback);
                    return this;
                }

                @Override
                public CollectionUpdater<E, H> removeAll(Collection<H> objects) {
                    checkValid();
                    if (objects.stream().anyMatch(Objects::isNull)) {
                        throw new StoreException("null values are not authorized for relations");
                    }
                    Runnable rollback = m_entityStore.updateCollectionRemoveAll(m_object, getPropertyName(accessor), objects);
                    m_rollbackActions.add(rollback);
                    return this;
                }

                @Override
                public CollectionUpdater<E, H> clear() {
                    checkValid();
                    Runnable rollback = m_entityStore.updateCollectionClear(m_object, getPropertyName(accessor));
                    m_rollbackActions.add(rollback);
                    return this;
                }
            };
        }

        @Override
        public E done() {
            checkValid();
            m_valid = false;
            m_activeObjectUpdaterCount.decrementAndGet();
            return m_object;
        }

        private void checkValid() {
            checkState(m_valid, "This UpdateObjectUpdater is no more usable");
        }

        private <V> String getPropertyName(Accessor<E, V> accessor) {
            return m_introspectionHelper.getMethodName(m_entityStore.getEntityDefinition().getEntityClass(), accessor);
        }
    }
}
