package io.axway.iron.core.internal;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.function.*;
import javax.annotation.*;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import io.axway.alf.log.Logger;
import io.axway.alf.log.LoggerFactory;
import io.axway.iron.Command;
import io.axway.iron.ReadOnlyTransaction;
import io.axway.iron.Store;
import io.axway.iron.StoreManager;
import io.axway.iron.core.internal.command.CommandProxyFactory;
import io.axway.iron.core.internal.definition.command.CommandDefinition;
import io.axway.iron.core.internal.definition.entity.EntityDefinition;
import io.axway.iron.core.internal.definition.entity.RelationDefinition;
import io.axway.iron.core.internal.entity.EntityStore;
import io.axway.iron.core.internal.entity.EntityStores;
import io.axway.iron.core.internal.entity.RelationStore;
import io.axway.iron.core.internal.transaction.ReadOnlyTransactionImpl;
import io.axway.iron.core.internal.transaction.ReadWriteTransactionImpl;
import io.axway.iron.core.internal.utils.IntrospectionHelper;
import io.axway.iron.error.MalformedCommandException;
import io.axway.iron.error.UnrecoverableStoreException;
import io.axway.iron.functional.Accessor;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.reactivex.flowables.ConnectableFlowable;

import static io.axway.alf.assertion.Assertion.*;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.*;

class StoreManagerImpl implements StoreManager {
    private static final Logger LOG = LoggerFactory.getLogger(StoreManagerImpl.class);

    private final TransactionStore m_transactionStore;
    private final IntrospectionHelper m_introspectionHelper;
    private final CommandProxyFactory m_commandProxyFactory;
    private final Map<Class<?>, EntityDefinition<?>> m_entityDefinitions;
    private final StorePersistence m_storePersistence;

    private final Cache<String, CompletableFuture<List<Object>>> m_futuresBySynchronizationId = CacheBuilder.newBuilder().weakValues().build();
    private final SnapshotStore m_snapshotStore;

    private BigInteger m_currentTxId = BigInteger.ONE.negate();
    private BigInteger m_lastSnapshotTxId = BigInteger.ONE.negate();

    private Disposable m_disposableTxFlow;
    private volatile boolean m_closed = false;

    private final Cache<String, StoreImpl> m_stores = CacheBuilder.newBuilder().build();

    StoreManagerImpl(TransactionSerializer transactionSerializer, TransactionStore transactionStore, SnapshotSerializer snapshotSerializer,
                     SnapshotStore snapshotStore, IntrospectionHelper introspectionHelper, CommandProxyFactory commandProxyFactory,
                     Collection<CommandDefinition<? extends Command<?>>> commandDefinitions, Map<Class<?>, EntityDefinition<?>> entityDefinitions) {
        m_transactionStore = transactionStore;
        m_introspectionHelper = introspectionHelper;
        m_commandProxyFactory = commandProxyFactory;
        m_entityDefinitions = entityDefinitions;
        m_snapshotStore = snapshotStore;
        m_storePersistence = new StorePersistence(m_commandProxyFactory, m_transactionStore, transactionSerializer, m_snapshotStore, snapshotSerializer,
                                                  commandDefinitions);

        m_storePersistence                                                           //
                .loadStores(storeName -> {
                    StoreImpl store = createStore(storeName);
                    m_stores.put(storeName, store);
                    return store.entityStores();
                })                                                                  //
                .ifPresent(lastTx -> {
                    m_currentTxId = lastTx;
                    m_lastSnapshotTxId = lastTx;
                    m_transactionStore.seekTransaction(lastTx);
                });

        ConnectableFlowable<StorePersistence.TransactionToExecute> connectableTransactions = m_storePersistence.allTransactions() //
                .publish();
        Flowable<StorePersistence.TransactionToExecute> transactions
                = connectableTransactions // to start only when connect is called and not with the first subscription
                .share(); // because we subscribe the main consumer and the one in charge of notifying recovery done

        Completable withTimeout = transactions.takeWhile(t -> !m_closed).timeout(1, SECONDS).ignoreElements();

        m_disposableTxFlow = transactions.subscribe(transaction -> {
            BigInteger txId = transaction.getTxId();
            CompletableFuture<List<Object>> transactionFuture = m_futuresBySynchronizationId.getIfPresent(transaction.getSynchronizationId());
            // if m_currentTxId == 0, this is the particular case of a "bootstrap snapshot" loaded at the very first start (i.e. a snapshot that does not come from passed transactions).
            // in this case, the first txId may be 0 but we don't want to skip it
            if (txId.compareTo(m_currentTxId) > 0 || m_currentTxId.equals(BigInteger.ZERO)) {
                List<Command<?>> commands = transaction.getCommands();
                Object[] results = new Object[commands.size()];
                Throwable error = null;
                try {
                    StoreImpl store = getStore(transaction.getStoreName());
                    ReadWriteTransactionImpl tx = new ReadWriteTransactionImpl(m_introspectionHelper, store.entityStores());
                    store.m_writeLock.lock();
                    try {
                        for (int i = 0; i < commands.size(); i++) {
                            Command<?> command = commands.get(i);
                            results[i] = command.execute(tx);
                            int activeObjectUpdaterCount = tx.getActiveObjectUpdaterCount();
                            if (activeObjectUpdaterCount > 0) {
                                String commandName = m_commandProxyFactory.getCommandName(command);
                                throw new MalformedCommandException(
                                        "Command leaves some active ObjectUpdater. Command need to be fixed. Transaction has been rollbacked",
                                        args -> args.add("commandName", commandName).add("activeObjectUpdaterCount", activeObjectUpdaterCount));
                            }
                        }
                    } catch (Exception e) {
                        error = e;
                        tx.rollback();
                        LOG.info("Transaction failed and rollbacked", args -> args.add("transactionId", txId), error);
                    } finally {
                        m_currentTxId = txId;
                        store.m_writeLock.unlock();
                    }
                } catch (Exception e) {
                    error = e;
                    LOG.info("Error processing transaction", args -> args.add("transactionId", txId), error);
                }

                if (transactionFuture != null) {
                    if (error != null) {
                        transactionFuture.completeExceptionally(error);
                    } else {
                        transactionFuture.complete(Arrays.asList(results));
                    }
                }
            } else {
                LOG.error("Transaction was already processed and will be ignored",
                          args -> args.add("transactionId", txId).add("latestProcessedTransactionId", m_currentTxId));
                if (transactionFuture != null) {
                   transactionFuture.complete(emptyList()); // do not block anyway
                }
            }
        }, error -> {
            LOG.info("Error processing transaction", error);
            m_futuresBySynchronizationId.asMap().values().forEach(f -> f.completeExceptionally(error));
        });

        connectableTransactions.connect();
        // use timeout to wait for
        Throwable error = withTimeout.blockingGet();
        if (!(error instanceof TimeoutException) && !(error instanceof NoSuchElementException)) {
            throw new UnrecoverableStoreException(error);
        }
    }

    @Override
    public Set<String> listStores() {
        return Collections.unmodifiableSet(m_stores.asMap().keySet());
    }

    @Nullable
    @Override
    public BigInteger snapshot() {
        ensureOpen();

        if (m_currentTxId.compareTo(m_lastSnapshotTxId) > 0) {
            BigInteger tx = m_currentTxId;
            m_stores.asMap().forEach((storeName, store) -> {
                store.m_readLock.lock();
                try {
                    m_storePersistence.persistSnapshot(tx, storeName, store.entityStores().toList());
                } finally {
                    store.m_readLock.unlock();
                }
            });
            m_lastSnapshotTxId = tx;
            return tx;
        } else {
            return null;
        }
    }

    @Override
    public BigInteger lastSnapshotTransactionId() {
        return m_lastSnapshotTxId;
    }

    @Override
    public StoreImpl getStore(String storeName) {
        ensureOpen();
        StoreImpl store = m_stores.getIfPresent(storeName);
        if (store != null) {
            return store;
        }

        AtomicBoolean newStore = new AtomicBoolean(false);
        try {
            store = m_stores.get(storeName, () -> {
                newStore.set(true);
                return createStore(storeName);
            });
        } catch (ExecutionException e) {
            throw new UnrecoverableStoreException(e);
        }
        return store;
    }

    private StoreImpl createStore(String storeName) {
        checkArgument(STORE_NAME_VALIDATOR_PATTERN.matcher(storeName).matches(), "Invalid store name", args -> args.add("storeName", storeName));
        EntityStores entityStores = createEntityStores();
        return new StoreImpl(storeName, entityStores);
    }

    private EntityStores createEntityStores() {
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

        return new EntityStores(entityStores.values());
    }

    private <E> EntityStore<E> createEntityStore(EntityDefinition<E> entityDefinition, Map<RelationDefinition, RelationStore> relationStores) {
        return new EntityStore<>(entityDefinition, relationStores);
    }

    @Override
    public void close() {
        LOG.debug("A store manager is going to be closed");
        ensureOpen();
        //TODO RRE : why not taking a snapshot before close ?
        m_closed = true;
        m_disposableTxFlow.dispose();
        m_transactionStore.close();
        m_snapshotStore.close();
    }

    private void ensureOpen() {
        checkState(!m_closed, "Store has been closed");
    }

    private class StoreImpl implements Store {
        private final String m_storeName;
        private final ReadOnlyTransactionImpl m_readOnlyTransaction;
        private final EntityStores m_entityStores;
        private final ReadWriteLock m_readWriteLock = new ReentrantReadWriteLock();
        private final Lock m_readLock = m_readWriteLock.readLock();
        private final Lock m_writeLock = m_readWriteLock.writeLock();

        private StoreImpl(String storeName, EntityStores entityStores) {
            m_storeName = storeName;
            m_readOnlyTransaction = new ReadOnlyTransactionImpl(m_introspectionHelper, entityStores);
            m_entityStores = entityStores;
        }

        @Override
        public TransactionBuilder begin() {
            ensureOpen();
            return new TransactionBuilderImpl(m_storeName);
        }

        @Override
        public <C extends Command<T>, T> CommandBuilder<C, T> createCommand(Class<C> commandClass) {
            ensureOpen();
            TransactionBuilder transactionBuilder = new TransactionBuilderImpl(m_storeName);
            CommandBuilder<C, T> commandBuilder = transactionBuilder.addCommand(commandClass);
            return new CommandBuilder<C, T>() {
                @Override
                public <V> CommandBuilderValueSetter<C, T, V> set(Accessor<C, V> accessor) {
                    CommandBuilderValueSetter<C, T, V> setter = commandBuilder.set(accessor);
                    return value -> {
                        setter.to(value);
                        return this;
                    };
                }

                @Override
                public CommandBuilder<C, T> map(Object parameters) {
                    commandBuilder.map(parameters);
                    return this;
                }

                @Override
                public Future<T> submit() {
                    Future<T> commandFuture = commandBuilder.submit();
                    transactionBuilder.submit();
                    return commandFuture;
                }
            };
        }

        @Override
        public void query(Consumer<ReadOnlyTransaction> storeQuery) {
            ensureOpen();
            m_readLock.lock();
            try {
                storeQuery.accept(m_readOnlyTransaction);
            } finally {
                m_readLock.unlock();
            }
        }

        @Override
        public <T> T query(Function<ReadOnlyTransaction, T> storeQuery) {
            ensureOpen();
            m_readLock.lock();
            try {
                return storeQuery.apply(m_readOnlyTransaction);
            } finally {
                m_readLock.unlock();
            }
        }

        private EntityStores entityStores() {
            return m_entityStores;
        }
    }

    private class TransactionBuilderImpl implements Store.TransactionBuilder {
        private final String m_storeName;
        private final String m_synchronizationId = UUID.randomUUID().toString();
        private final CompletableFuture<List<Object>> m_future = new CompletableFuture<>();
        private final List<Command<?>> m_commands = new ArrayList<>();
        private boolean m_valid = true;

        private TransactionBuilderImpl(String storeName) {
            m_storeName = storeName;
        }

        private void checkValid() {
            checkState(m_valid, "This TransactionBuilder is no more usable");
        }

        @Override
        public <C extends Command<T>, T> Store.CommandBuilder<C, T> addCommand(Class<C> commandClass) {
            checkValid();
            return new CommandBuilderImpl<>(this, commandClass);
        }

        @Override
        public Future<List<Object>> submit() {
            checkValid();
            m_valid = false;
            m_futuresBySynchronizationId.put(m_synchronizationId, m_future);
            m_storePersistence.persistTransaction(m_storeName, m_synchronizationId, m_commands);

            return m_future;
        }
    }

    private class CommandBuilderImpl<C extends Command<T>, T> implements Store.CommandBuilder<C, T> {
        private final TransactionBuilderImpl m_transactionBuilder;
        private final Class<C> m_commandClass;
        private final Map<String, Object> m_parameters = new HashMap<>();
        private boolean m_valid = true;

        private CommandBuilderImpl(TransactionBuilderImpl transactionBuilder, Class<C> commandClass) {
            m_transactionBuilder = transactionBuilder;
            m_commandClass = commandClass;
        }

        private void checkValid() {
            checkState(m_valid && m_transactionBuilder.m_valid, "This CommandBuilder is no more usable");
        }

        private void setParameter(String parameterName, Object value) {
            // TODO ensure value class
            checkState(m_parameters.putIfAbsent(parameterName, value) == null, "Command parameter is already set",
                       args -> args.add("parameterName", parameterName));
        }

        @Override
        public <V> Store.CommandBuilderValueSetter<C, T, V> set(Accessor<C, V> accessor) {
            checkValid();
            String parameterName = m_introspectionHelper.getMethodName(m_commandClass, accessor);
            return value -> {
                setParameter(parameterName, value);
                return CommandBuilderImpl.this;
            };
        }

        @Override
        public Store.CommandBuilder<C, T> map(Object parameters) {
            checkValid();
            if (parameters instanceof Map) {
                Map<?, ?> map = (Map<?, ?>) parameters;
                for (Map.Entry<?, ?> e : map.entrySet()) {
                    String key = (String) e.getKey();
                    setParameter(key, e.getValue());
                }
            } else {
                throw new UnsupportedOperationException("To be implemented"); //TODO implement
            }
            return this;
        }

        @Override
        public Future<T> submit() {
            checkValid();
            m_valid = false;
            C command = m_commandProxyFactory.createCommand(m_commandClass, m_parameters);
            int index = m_transactionBuilder.m_commands.size();
            m_transactionBuilder.m_commands.add(command);

            return new CommandFutureWrapper<>(m_transactionBuilder.m_future, index);
        }
    }

    /**
     * Directly send every calls to the wrapped {@code Future}. This class is needed to retain a strong reference to the underlying transaction {@code Future}.<br>
     * So it prevents the transaction {@code Future} to be evicted from the {@link #m_futuresBySynchronizationId} weak cache, giving the opportunity to the
     * transaction {@code Future} to be completed, and so the command {@code Future} can complete also.
     *
     * @param <T> the wrapper future type
     */
    private static final class CommandFutureWrapper<T> implements Future<T> {
        /**
         * Strong reference to the transaction {@code Future}. Not need to be used.
         */
        @SuppressWarnings("unused")
        private final Object m_txFuture;
        private final Future<T> m_commandFuture;

        CommandFutureWrapper(CompletableFuture<List<Object>> txFuture, int commandIndex) {
            m_txFuture = txFuture;
            //noinspection unchecked
            m_commandFuture = txFuture.thenApply(values -> (T) values.get(commandIndex));
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return m_commandFuture.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return m_commandFuture.isCancelled();
        }

        @Override
        public boolean isDone() {
            return m_commandFuture.isDone();
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            return m_commandFuture.get();
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return m_commandFuture.get(timeout, unit);
        }
    }
}
