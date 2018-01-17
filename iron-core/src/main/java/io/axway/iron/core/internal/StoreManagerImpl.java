package io.axway.iron.core.internal;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.function.*;
import javax.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.axway.iron.Command;
import io.axway.iron.ReadOnlyTransaction;
import io.axway.iron.Store;
import io.axway.iron.StoreManager;
import io.axway.iron.core.internal.command.CommandProxyFactory;
import io.axway.iron.core.internal.entity.EntityStoreManager;
import io.axway.iron.core.internal.transaction.ReadOnlyTransactionImpl;
import io.axway.iron.core.internal.transaction.ReadWriteTransactionImpl;
import io.axway.iron.core.internal.utils.IntrospectionHelper;
import io.axway.iron.error.StoreException;
import io.axway.iron.functional.Accessor;

import static com.google.common.base.Preconditions.checkState;

class StoreManagerImpl implements StoreManager {
    private static final Logger LOG = LoggerFactory.getLogger(io.axway.iron.core.internal.StoreManagerImpl.class);

    private final IntrospectionHelper m_introspectionHelper;
    private final CommandProxyFactory m_commandProxyFactory;
    private final StorePersistence m_storePersistence;
    private final EntityStoreManager m_entityStoreManager;
    private final Runnable m_onClose;
    private final ReadOnlyTransaction m_readOnlyTransaction;
    private final Thread m_thread;
    private final StoreImpl m_store = new StoreImpl();
    private final Cache<String, CompletableFuture<List<?>>> m_futuresBySynchronizationId = CacheBuilder.newBuilder().weakValues().build();
    private final CountDownLatch m_transactionRecoveryDone = new CountDownLatch(1);

    private final ReadWriteLock m_readWriteLock = new ReentrantReadWriteLock();
    private final Lock m_readLock = m_readWriteLock.readLock();
    private final Lock m_writeLock = m_readWriteLock.writeLock();
    private BigInteger m_currentTxId = BigInteger.ZERO;
    private BigInteger m_lastSnapshotTxId = BigInteger.ONE.negate();

    private volatile boolean m_closed = false;

    StoreManagerImpl(IntrospectionHelper introspectionHelper, CommandProxyFactory commandProxyFactory, StorePersistence storePersistence,
                     EntityStoreManager entityStoreManager, String storeName, Runnable onClose) {
        m_introspectionHelper = introspectionHelper;
        m_commandProxyFactory = commandProxyFactory;
        m_storePersistence = storePersistence;
        m_entityStoreManager = entityStoreManager;
        m_onClose = onClose;
        m_readOnlyTransaction = new ReadOnlyTransactionImpl(m_introspectionHelper, m_entityStoreManager);
        m_thread = new Thread(this::consumerLoop, "IronConsumer-" + storeName);
        m_thread.setUncaughtExceptionHandler((t, e) -> LOG.error("Transaction consumer thread failure", e));
    }

    void open() {
        ensureOpen();
        BigInteger recoveredSnapshotTxId = m_storePersistence.recover(m_entityStoreManager::getEntityStore);
        m_thread.start();
        if (recoveredSnapshotTxId != null) {
            m_currentTxId = recoveredSnapshotTxId;
            m_lastSnapshotTxId = m_currentTxId;
        } else {
            // create the first snapshot with an empty transaction
            try {
                m_store.begin().submit().get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StoreException(e);
            } catch (ExecutionException e) {
                throw new StoreException(e);
            }
            snapshot();
        }

        try {
            m_transactionRecoveryDone.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StoreException(e);
        }
    }

    @Override
    public Store getStore() {
        ensureOpen();
        return m_store;
    }

    @Nullable
    @Override
    public BigInteger snapshot() {
        ensureOpen();
        m_readLock.lock();
        try {
            if (m_currentTxId.compareTo(m_lastSnapshotTxId) > 0) {
                m_storePersistence.persistSnapshot(m_currentTxId, m_entityStoreManager.getEntityStores());
                m_lastSnapshotTxId = m_currentTxId;
                return m_lastSnapshotTxId;
            } else {
                return null;
            }
        } finally {
            m_readLock.unlock();
        }
    }

    @Override
    public void close() {
        ensureOpen();
        m_closed = true;
        try {
            m_thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        m_onClose.run();
    }

    @Override
    public BigInteger lastSnapshotTransactionId() {
        return m_lastSnapshotTxId;
    }

    private void ensureOpen() {
        checkState(!m_closed, "Store has been closed");
    }

    private void consumerLoop() {
        boolean shouldNotifyTransactionDone = true;
        while (!m_closed) {
            StorePersistence.TransactionToExecute transactionToExecute = m_storePersistence.pollNextTransaction(10);
            if (transactionToExecute != null) {
                BigInteger txId = transactionToExecute.getTxId();
                List<Command<?>> commands = transactionToExecute.getCommands();
                Object[] results = new Object[commands.size()];
                Throwable error = null;
                ReadWriteTransactionImpl tx = new ReadWriteTransactionImpl(m_introspectionHelper, m_entityStoreManager);
                m_writeLock.lock();
                try {
                    for (int i = 0; i < commands.size(); i++) {
                        Command<?> command = commands.get(i);
                        results[i] = command.execute(tx);
                        int activeObjectUpdaterCount = tx.getActiveObjectUpdaterCount();
                        if (activeObjectUpdaterCount > 0) {
                            String commandName = m_commandProxyFactory.getCommandName(command);
                            throw new IllegalStateException("Command '" + commandName + "' leaves " + activeObjectUpdaterCount
                                                                    + " active ObjectUpdater. Command need to be fixed, transaction rollback");
                        }
                    }
                } catch (Exception e) {
                    error = e;
                    tx.rollback();
                } finally {
                    m_currentTxId = txId;
                    m_writeLock.unlock();
                }

                if (error != null) {
                    LOG.info("Transaction failed and rollbacked {transactionId={}}", txId, error);
                }

                String synchronizationId = transactionToExecute.getSynchronizationId();
                CompletableFuture<List<?>> transactionFuture = m_futuresBySynchronizationId.getIfPresent(synchronizationId);
                if (transactionFuture != null) {
                    if (error != null) {
                        transactionFuture.completeExceptionally(error);
                    } else {
                        transactionFuture.complete(Arrays.asList(results));
                    }
                }
            } else if (shouldNotifyTransactionDone) {
                shouldNotifyTransactionDone = false;
                m_transactionRecoveryDone.countDown();
            }
        }
    }

    private class StoreImpl implements Store {

        @Override
        public TransactionBuilder begin() {
            ensureOpen();
            return new TransactionBuilderImpl();
        }

        @Override
        public <C extends Command<T>, T> CommandBuilder<C, T> createCommand(Class<C> commandClass) {
            ensureOpen();
            TransactionBuilder transactionBuilder = new TransactionBuilderImpl();
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
    }

    private class TransactionBuilderImpl implements Store.TransactionBuilder {
        private final String m_synchronizationId = UUID.randomUUID().toString();
        private final CompletableFuture<List<?>> m_future = new CompletableFuture<>();
        private final List<Command<?>> m_commands = new ArrayList<>();
        private boolean m_valid = true;

        private void checkValid() {
            checkState(m_valid, "This TransactionBuilder is no more usable");
        }

        @Override
        public <C extends Command<T>, T> Store.CommandBuilder<C, T> addCommand(Class<C> commandClass) {
            checkValid();
            return new CommandBuilderImpl<>(this, commandClass);
        }

        @Override
        public Future<List<?>> submit() {
            checkValid();
            m_valid = false;
            m_futuresBySynchronizationId.put(m_synchronizationId, m_future);
            m_storePersistence.persistTransaction(m_synchronizationId, m_commands);

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
            checkState(m_parameters.putIfAbsent(parameterName, value) == null, "Command parameter %s is already set", parameterName);
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
                throw new UnsupportedOperationException("To be implemented"); //TODO implements
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

        CommandFutureWrapper(CompletableFuture<List<?>> txFuture, int commandIndex) {
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
