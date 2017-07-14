package io.axway.iron.core.internal;

import java.util.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.axway.iron.Command;
import io.axway.iron.StoreManagerFactory;
import io.axway.iron.core.StoreManagerFactoryBuilder;
import io.axway.iron.core.internal.command.CommandProxyFactory;
import io.axway.iron.core.internal.definition.DataTypeManager;
import io.axway.iron.core.internal.definition.InterfaceValidator;
import io.axway.iron.core.internal.definition.command.CommandDefinition;
import io.axway.iron.core.internal.definition.command.CommandDefinitionBuilder;
import io.axway.iron.core.internal.definition.entity.EntityDefinition;
import io.axway.iron.core.internal.definition.entity.EntityDefinitionBuilder;
import io.axway.iron.core.internal.utils.IntrospectionHelper;
import io.axway.iron.core.internal.utils.proxy.ProxyConstructorFactory;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static com.google.common.base.Preconditions.checkState;

public class StoreManagerFactoryBuilderImpl implements StoreManagerFactoryBuilder {

    private final Set<Class<? extends Command<?>>> m_commandClasses = new HashSet<>();
    private final Set<Class<?>> m_entityClasses = new HashSet<>();

    private TransactionSerializer m_transactionSerializer;
    private TransactionStoreFactory m_transactionStoreFactory;
    private SnapshotSerializer m_snapshotSerializer;
    private SnapshotStoreFactory m_snapshotStoreFactory;

    @Override
    public StoreManagerFactoryBuilderImpl withEntityClass(Class<?> entityClass) {
        checkState(m_entityClasses.add(entityClass), "Entity class %s has been already added", entityClass.getName());
        return this;
    }

    @Override
    public <T> StoreManagerFactoryBuilderImpl withCommandClass(Class<? extends Command<T>> commandClass) {
        checkState(m_commandClasses.add(commandClass), "Command class %s has been already added", commandClass.getName());
        return this;
    }

    @Override
    public StoreManagerFactoryBuilderImpl withTransactionSerializer(TransactionSerializer transactionSerializer) {
        checkState(m_transactionSerializer == null, "Transaction serializer has been already set");
        m_transactionSerializer = transactionSerializer;
        return this;
    }

    @Override
    public StoreManagerFactoryBuilder withTransactionStoreFactory(TransactionStoreFactory transactionStoreFactory) {
        checkState(m_transactionStoreFactory == null, "Transaction store factory has been already set");
        m_transactionStoreFactory = transactionStoreFactory;
        return this;
    }

    @Override
    public StoreManagerFactoryBuilderImpl withSnapshotSerializer(SnapshotSerializer snapshotSerializer) {
        checkState(m_snapshotSerializer == null, "Snapshot serializer has been already set");
        m_snapshotSerializer = snapshotSerializer;
        return this;
    }

    @Override
    public StoreManagerFactoryBuilder withSnapshotStoreFactory(SnapshotStoreFactory snapshotStoreFactory) {
        checkState(m_snapshotStoreFactory == null, "Snapshot store has been already set");
        m_snapshotStoreFactory = snapshotStoreFactory;
        return this;
    }

    @Override
    public StoreManagerFactory build() {
        checkState(m_transactionSerializer != null, "Transaction serializer has not been specified");
        checkState(m_transactionStoreFactory != null, "Transaction store factory has not been specified");
        checkState(m_snapshotSerializer != null, "Snapshot serializer has not been specified");
        checkState(m_snapshotStoreFactory != null, "Snapshot store factory has not been specified");

        IntrospectionHelper introspectionHelper = new IntrospectionHelper();
        ProxyConstructorFactory proxyConstructorFactory = new ProxyConstructorFactory();
        DataTypeManager dataTypeManager = new DataTypeManager();
        InterfaceValidator interfaceValidator = new InterfaceValidator(introspectionHelper, dataTypeManager);
        CommandDefinitionBuilder commandDefinitionBuilder = new CommandDefinitionBuilder(proxyConstructorFactory, dataTypeManager, interfaceValidator);
        EntityDefinitionBuilder entityDefinitionBuilder = new EntityDefinitionBuilder(introspectionHelper, proxyConstructorFactory, dataTypeManager,
                                                                                      interfaceValidator);

        Collection<CommandDefinition<? extends Command<?>>> commandDefinitions = buildCommandDefinitions(commandDefinitionBuilder);
        Map<Class<?>, EntityDefinition<?>> entityDefinitions = entityDefinitionBuilder.analyzeEntities(ImmutableSet.copyOf(m_entityClasses));

        CommandProxyFactory commandProxyFactory = new CommandProxyFactory(commandDefinitions);

        return new StoreManagerFactoryImpl(m_transactionSerializer, m_transactionStoreFactory, m_snapshotSerializer, m_snapshotStoreFactory,
                                           introspectionHelper, commandProxyFactory, commandDefinitions, entityDefinitions);
    }

    private Collection<CommandDefinition<? extends Command<?>>> buildCommandDefinitions(CommandDefinitionBuilder commandDefinitionBuilder) {
        ImmutableList.Builder<CommandDefinition<? extends Command<?>>> definitions = ImmutableList.builder();
        m_commandClasses.forEach(commandClass -> definitions.add(commandDefinitionBuilder.analyzeCommandClass(commandClass)));
        return definitions.build();
    }
}
