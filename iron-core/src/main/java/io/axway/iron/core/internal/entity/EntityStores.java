package io.axway.iron.core.internal.entity;

import java.util.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.axway.iron.error.StoreException;

public class EntityStores {
    private final List<EntityStore<?>> m_entityStores;
    private final Map<Class<?>, EntityStore<?>> m_entityStoresByClass;
    private final Map<String, EntityStore<?>> m_entityStoresByName;

    public EntityStores(Collection<EntityStore<?>> unsortedEntityStores) {
        List<EntityStore<?>> entityStores = new ArrayList<>(unsortedEntityStores);
        entityStores.sort(Comparator.comparing(s -> s.getEntityDefinition().getEntityName()));
        m_entityStores = ImmutableList.copyOf(entityStores);

        ImmutableMap.Builder<Class<?>, EntityStore<?>> entityStoresByClass = ImmutableMap.builder();
        ImmutableMap.Builder<String, EntityStore<?>> entityStoresByName = ImmutableMap.builder();
        m_entityStores.forEach(entityStore -> {
            entityStoresByClass.put(entityStore.getEntityDefinition().getEntityClass(), entityStore);
            entityStoresByName.put(entityStore.getEntityDefinition().getEntityName(), entityStore);
        });
        m_entityStoresByClass = entityStoresByClass.build();
        m_entityStoresByName = entityStoresByName.build();
    }

    public List<EntityStore<?>> toList() {
        return m_entityStores;
    }

    public <E> EntityStore<E> getEntityStore(Class<E> entityClass) {
        EntityStore<?> entityStore = m_entityStoresByClass.get(entityClass);
        if (entityStore == null) {
            throw new StoreException("Unknown entity class", args -> args.add("entityClass", entityClass.getName()));
        }
        //noinspection unchecked
        return (EntityStore<E>) entityStore;
    }

    public EntityStore<?> getEntityStore(String entityName) {
        EntityStore<?> entityStore = m_entityStoresByName.get(entityName);
        if (entityStore == null) {
            throw new StoreException("Unknown entity name", args -> args.add("entityName", entityName));
        }
        return entityStore;
    }

    public <E> EntityStore<E> getEntityStoreByObject(E object) {
        InstanceProxy instance = (InstanceProxy) object;
        return getEntityStore(instance.__entityClass());
    }
}
