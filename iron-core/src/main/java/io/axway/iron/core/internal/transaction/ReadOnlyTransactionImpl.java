package io.axway.iron.core.internal.transaction;

import java.util.*;
import java.util.stream.*;
import javax.annotation.*;
import io.axway.iron.ReadOnlyTransaction;
import io.axway.iron.core.internal.entity.EntityStore;
import io.axway.iron.core.internal.entity.EntityStores;
import io.axway.iron.core.internal.utils.IntrospectionHelper;
import io.axway.iron.error.ObjectNotFoundException;
import io.axway.iron.functional.Accessor;

public class ReadOnlyTransactionImpl implements ReadOnlyTransaction {
    final IntrospectionHelper m_introspectionHelper;
    final EntityStores m_entityStores;

    public ReadOnlyTransactionImpl(IntrospectionHelper introspectionHelper, EntityStores entityStores) {
        m_introspectionHelper = introspectionHelper;
        m_entityStores = entityStores;
    }

    @Override
    public <E> From<E> select(Class<E> entityClass) {
        return new From<E>() {
            private final EntityStore<E> m_entityStore = m_entityStores.getEntityStore(entityClass);

            @Override
            public Collection<E> all() {
                return m_entityStore.list();
            }

            @Override
            public <V> On<E, V> where(Accessor<E, V> accessor) {
                return new On<E, V>() {
                    private final String m_propertyName = m_introspectionHelper.getMethodName(entityClass, accessor);

                    @Override
                    public E equalsTo(V value) {
                        E object = equalsToOrNull(value);
                        if (object != null) {
                            return object;
                        } else {
                            throw new ObjectNotFoundException(m_entityStore.getEntityDefinition().getEntityName(), m_propertyName, value);
                        }
                    }

                    @Nullable
                    @Override
                    public E equalsToOrNull(V value) {
                        return m_entityStore.getByUnique(m_propertyName, value);
                    }

                    @Override
                    public Collection<E> allContainedIn(Collection<V> values) {
                        return values.stream().map(this::equalsTo).collect(Collectors.toList());
                    }

                    @Override
                    public Collection<E> someContainedIn(Collection<V> values) {
                        return values.stream().map(this::equalsToOrNull).filter(Objects::nonNull).collect(Collectors.toList());
                    }
                };
            }
        };
    }
}
