package io.axway.iron.core.internal.transaction;

import java.util.*;
import java.util.concurrent.atomic.*;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.core.internal.entity.EntityStore;
import io.axway.iron.core.internal.entity.EntityStoreManager;
import io.axway.iron.core.internal.utils.IntrospectionHelper;
import io.axway.iron.functional.Accessor;

import static com.google.common.base.Preconditions.checkState;

public class ReadWriteTransactionImpl extends ReadOnlyTransactionImpl implements ReadWriteTransaction {
    private final List<Runnable> m_rollbackActions = new ArrayList<>();
    private final AtomicInteger m_activeObjectUpdaterCount = new AtomicInteger();

    public ReadWriteTransactionImpl(IntrospectionHelper introspectionHelper, EntityStoreManager entityStoreManager) {
        super(introspectionHelper, entityStoreManager);
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
        EntityStore<E> entityStore = m_entityStoreManager.getEntityStore(entityClass);
        E object = entityStore.newInstance();
        return new InsertObjectUpdater<>(entityStore, object);
    }

    @Override
    public <E> ObjectUpdater<E> update(E object) {
        EntityStore<E> entityStore = m_entityStoreManager.getEntityStoreByObject(object);
        return new UpdateObjectUpdater<>(entityStore, object);
    }

    @Override
    public void delete(Object object) {
        EntityStore<Object> entityStore = m_entityStoreManager.getEntityStoreByObject(object);
        entityStore.delete(object);
        m_rollbackActions.add(() -> entityStore.undelete(object));
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

        private void checkValid() {
            checkState(m_valid, "This InsertObjectUpdater is no more usable");
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
            E object = m_entityStore.insert(m_object);
            m_rollbackActions.add(() -> m_entityStore.delete(m_object));
            m_activeObjectUpdaterCount.decrementAndGet();
            return object;
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

        private void checkValid() {
            checkState(m_valid, "This UpdateObjectUpdater is no more usable");
        }

        @Override
        public <V> To<E, V> set(Accessor<E, V> accessor) {
            checkValid();
            return value -> {
                checkValid();
                String propertyName = m_introspectionHelper.getMethodName(m_entityStore.getEntityDefinition().getEntityClass(), accessor);
                V oldValue = m_entityStore.update(m_object, propertyName, value);
                m_rollbackActions.add(() -> m_entityStore.update(m_object, propertyName, oldValue));
                return UpdateObjectUpdater.this;
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
            m_activeObjectUpdaterCount.decrementAndGet();
            return m_object;
        }
    }
}
