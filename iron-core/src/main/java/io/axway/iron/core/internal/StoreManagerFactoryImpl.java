package io.axway.iron.core.internal;

import java.util.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.axway.iron.Command;
import io.axway.iron.StoreManager;
import io.axway.iron.StoreManagerFactory;
import io.axway.iron.core.internal.command.CommandProxyFactory;
import io.axway.iron.core.internal.definition.command.CommandDefinition;
import io.axway.iron.core.internal.definition.entity.EntityDefinition;
import io.axway.iron.core.internal.definition.entity.RelationDefinition;
import io.axway.iron.core.internal.entity.EntityStore;
import io.axway.iron.core.internal.entity.EntityStoreManager;
import io.axway.iron.core.internal.entity.RelationStore;
import io.axway.iron.core.internal.utils.IntrospectionHelper;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStore;
import io.axway.iron.spi.storage.TransactionStoreFactory;

class StoreManagerFactoryImpl implements StoreManagerFactory {
    private final TransactionSerializer m_transactionSerializer;
    private final TransactionStoreFactory m_transactionStoreFactory;
    private final SnapshotSerializer m_snapshotSerializer;
    private final SnapshotStoreFactory m_snapshotStoreFactory;
    private final IntrospectionHelper m_introspectionHelper;
    private final CommandProxyFactory m_commandProxyFactory;
    private final Collection<CommandDefinition<? extends Command<?>>> m_commandDefinitions;
    private final Map<Class<?>, EntityDefinition<?>> m_entityDefinitions;
    private final Set<String> m_openedStores = Collections.synchronizedSet(new HashSet<>());

    StoreManagerFactoryImpl(TransactionSerializer transactionSerializer, TransactionStoreFactory transactionStoreFactory, SnapshotSerializer snapshotSerializer,
                            SnapshotStoreFactory snapshotStoreFactory, IntrospectionHelper introspectionHelper, CommandProxyFactory commandProxyFactory,
                            Collection<CommandDefinition<? extends Command<?>>> commandDefinitions, Map<Class<?>, EntityDefinition<?>> entityDefinitions) {
        m_transactionSerializer = transactionSerializer;
        m_transactionStoreFactory = transactionStoreFactory;
        m_snapshotSerializer = snapshotSerializer;
        m_snapshotStoreFactory = snapshotStoreFactory;
        m_introspectionHelper = introspectionHelper;
        m_commandProxyFactory = commandProxyFactory;
        m_commandDefinitions = commandDefinitions;
        m_entityDefinitions = entityDefinitions;
    }

    @Override
    public StoreManager openStore(String storeName) {
        Preconditions.checkArgument(STORE_NAME_VALIDATOR_PATTERN.matcher(storeName).matches(), "Invalid store name: %s", storeName);
        Preconditions.checkState(m_openedStores.add(storeName), "Store %s is already open, cannot open it twice", storeName);

        boolean success = false;
        try {
            StoreManager storeManager = createStore(storeName);
            success = true;
            return storeManager;
        } finally {
            if (!success) {
                m_openedStores.remove(storeName);
            }
        }
    }

    private StoreManager createStore(String storeName) {
        TransactionStore transactionStore = m_transactionStoreFactory.createTransactionStore(storeName);
        SnapshotStore snapshotStore = m_snapshotStoreFactory.createSnapshotStore(storeName);
        StorePersistence storePersistence = new StorePersistence(m_commandProxyFactory, transactionStore, m_transactionSerializer, snapshotStore,
                                                                 m_snapshotSerializer, m_commandDefinitions);

        ImmutableMap.Builder<RelationDefinition, RelationStore> relationStoresBuilder = ImmutableMap.builder();
        m_entityDefinitions.values().stream().flatMap(entityDefinition -> entityDefinition.getRelations().values().stream()).forEach(relationDefinition -> {
            RelationStore relationStore = RelationStore.newRelationStore(relationDefinition);
            relationStoresBuilder.put(relationDefinition, relationStore);
        });
        Map<RelationDefinition, RelationStore> relationStores = relationStoresBuilder.build();

        ImmutableMap.Builder<Class<?>, EntityStore<?>> entityStoresBuilder = ImmutableMap.builder();
        m_entityDefinitions.values().forEach(entityDefinition -> {
            EntityStore<?> entityStore = createEntityStore(entityDefinition, relationStores);
            entityStoresBuilder.put(entityDefinition.getEntityClass(), entityStore);
        });
        Map<Class<?>, EntityStore<?>> entityStores = entityStoresBuilder.build();

        for (EntityStore<?> entityStore : entityStores.values()) {
            entityStore.init(entityStores, relationStores);
        }

        EntityStoreManager entityStoreManager = new EntityStoreManager(entityStores.values());
        Runnable onClose = () -> m_openedStores.remove(storeName);

        StoreManagerImpl store = new StoreManagerImpl(m_introspectionHelper, m_commandProxyFactory, storePersistence, entityStoreManager, storeName, onClose);
        store.open();
        return store;
    }

    private <E> EntityStore<E> createEntityStore(EntityDefinition<E> entityDefinition, Map<RelationDefinition, RelationStore> relationStores) {
        return new EntityStore<>(entityDefinition, relationStores);
    }
}
