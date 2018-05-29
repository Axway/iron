package io.axway.iron.core.internal;

import java.util.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.axway.iron.Command;
import io.axway.iron.StoreManager;
import io.axway.iron.core.StoreManagerBuilder;
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
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

import static io.axway.alf.assertion.Assertion.checkState;

public class StoreManagerBuilderImpl implements StoreManagerBuilder {

    private final Set<Class<? extends Command<?>>> m_commandClasses = new HashSet<>();
    private final Set<Class<?>> m_entityClasses = new HashSet<>();
    private TransactionSerializer m_transactionSerializer;
    private TransactionStore m_transactionStore;
    private SnapshotSerializer m_snapshotSerializer;
    private SnapshotStore m_snapshotStore;

    public StoreManagerBuilderImpl() {
    }

    public StoreManagerBuilderImpl(String name, Properties properties) {
        new StoreManagerBuilderConfigurator().fill(this, name, properties);
    }

    @Override
    public StoreManagerBuilder withEntityClass(Class<?> entityClass) {
        checkState(m_entityClasses.add(entityClass), "Entity class has been already added", args -> args.add("entityClass", entityClass.getName()));
        return this;
    }

    @Override
    public <T> StoreManagerBuilder withCommandClass(Class<? extends Command<T>> commandClass) {
        checkState(m_commandClasses.add(commandClass), "Command class %s has been already added", args -> args.add("commandClass", commandClass.getName()));
        return this;
    }

    @Override
    public StoreManagerBuilder withTransactionSerializer(TransactionSerializer transactionSerializer) {
        checkState(m_transactionSerializer == null, "Transaction serializer has been already set");
        m_transactionSerializer = transactionSerializer;
        return this;
    }

    @Override
    public StoreManagerBuilder withTransactionStore(TransactionStore transactionStore) {
        checkState(m_transactionStore == null, "Transaction store factory has been already set");
        m_transactionStore = transactionStore;
        return this;
    }

    @Override
    public StoreManagerBuilder withSnapshotSerializer(SnapshotSerializer snapshotSerializer) {
        checkState(m_snapshotSerializer == null, "Snapshot serializer has been already set");
        m_snapshotSerializer = snapshotSerializer;
        return this;
    }

    @Override
    public StoreManagerBuilder withSnapshotStore(SnapshotStore snapshotStore) {
        checkState(m_snapshotStore == null, "Snapshot store has been already set");
        m_snapshotStore = snapshotStore;
        return this;
    }

    @Override
    public StoreManager build() {
        checkState(m_transactionSerializer != null, "Transaction serializer has not been specified");
        checkState(m_transactionStore != null, "Transaction store factory has not been specified");
        checkState(m_snapshotSerializer != null, "Snapshot serializer has not been specified");
        checkState(m_snapshotStore != null, "Snapshot store factory has not been specified");

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

        return new StoreManagerImpl(m_transactionSerializer, m_transactionStore, m_snapshotSerializer, m_snapshotStore,
                                    introspectionHelper, commandProxyFactory, commandDefinitions, entityDefinitions);
    }

    private Collection<CommandDefinition<? extends Command<?>>> buildCommandDefinitions(CommandDefinitionBuilder commandDefinitionBuilder) {
        ImmutableList.Builder<CommandDefinition<? extends Command<?>>> definitions = ImmutableList.builder();
        m_commandClasses.forEach(commandClass -> definitions.add(commandDefinitionBuilder.analyzeCommandClass(commandClass)));
        return definitions.build();
    }
}
