package io.axway.iron.core.internal;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;
import javax.annotation.*;
import com.google.common.collect.ImmutableMap;
import io.axway.alf.log.Logger;
import io.axway.alf.log.LoggerFactory;
import io.axway.iron.Command;
import io.axway.iron.core.internal.command.CommandProxyFactory;
import io.axway.iron.core.internal.definition.command.CommandDefinition;
import io.axway.iron.core.internal.entity.EntityStore;
import io.axway.iron.core.internal.entity.EntityStores;
import io.axway.iron.error.StoreException;
import io.axway.iron.error.UnrecoverableStoreException;
import io.axway.iron.spi.model.snapshot.SerializableSnapshot;
import io.axway.iron.spi.model.transaction.SerializableCommand;
import io.axway.iron.spi.model.transaction.SerializableTransaction;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;
import io.reactivex.Flowable;

import static io.axway.alf.assertion.Assertion.checkArgument;
import static io.axway.iron.spi.model.snapshot.SerializableSnapshot.SNAPSHOT_MODEL_VERSION;
import static io.axway.iron.spi.model.transaction.SerializableTransaction.TRANSACTION_MODEL_VERSION;

class StorePersistence {
    private static final Logger LOG = LoggerFactory.getLogger(StorePersistence.class);

    private final CommandProxyFactory m_commandProxyFactory;
    private final TransactionStore m_transactionStore;
    private final TransactionSerializer m_transactionSerializer;
    private final SnapshotStore m_snapshotStore;
    private final SnapshotSerializer m_snapshotSerializer;
    private final Map<String, CommandDefinition<? extends Command<?>>> m_commandDefinitions;

    StorePersistence(CommandProxyFactory commandProxyFactory, TransactionStore transactionStore, TransactionSerializer transactionSerializer,
                     SnapshotStore snapshotStore, SnapshotSerializer snapshotSerializer,
                     Collection<CommandDefinition<? extends Command<?>>> commandDefinitions) {
        m_commandProxyFactory = commandProxyFactory;
        m_transactionStore = transactionStore;
        m_transactionSerializer = transactionSerializer;
        m_snapshotStore = snapshotStore;
        m_snapshotSerializer = snapshotSerializer;

        ImmutableMap.Builder<String, CommandDefinition<? extends Command<?>>> commandDefinitionsBuilder = ImmutableMap.builder();
        commandDefinitions.forEach(commandDefinition -> commandDefinitionsBuilder.put(commandDefinition.getCommandClass().getName(), commandDefinition));
        m_commandDefinitions = commandDefinitionsBuilder.build();
    }

    void persistSnapshot(BigInteger txId, String storeName, Collection<EntityStore<?>> entityStores) {
        SerializableSnapshot serializableSnapshot = new SerializableSnapshot();
        serializableSnapshot.setSnapshotModelVersion(SNAPSHOT_MODEL_VERSION);
        serializableSnapshot.setTransactionId(txId);
        serializableSnapshot.setEntities(entityStores.stream().map(EntityStore::snapshot).collect(Collectors.toList()));

        try (OutputStream out = m_snapshotStore.createSnapshotWriter(storeName, txId)) {
            m_snapshotSerializer.serializeSnapshot(out, serializableSnapshot);
        } catch (IOException e) {
            throw new StoreException("Error when creating the store snapshot", args -> args.add("transactionId", txId), e);
        }
    }

    /**
     * Load the stores.
     *
     * @param entityStoresByStoreName the map store name to entity stores
     * @return latestSnapshotTxId the transaction id of the last snapshot
     */
    Optional<BigInteger> loadStores(Function<String, EntityStores> entityStoresByStoreName) {
        Optional<BigInteger> latestSnapshotTxId;
        try {
            latestSnapshotTxId = m_snapshotStore.listSnapshots().stream().max(BigInteger::compareTo);
        } catch (IOException e) {
            throw new UnrecoverableStoreException("Error occurred when recovering from latest snapshot", e);
        }

        latestSnapshotTxId.ifPresent(lastTx -> {
            LOG.info("Recovering store from snapshot", args -> args.add("transactionId", lastTx));

            try {
                Flowable.fromPublisher(m_snapshotStore.createSnapshotReader(lastTx))  //
                        .blockingForEach(reader -> {
                            String storeName = reader.storeName();
                            EntityStores entityStores = entityStoresByStoreName.apply(storeName);

                            SerializableSnapshot serializableSnapshot;
                            try (InputStream is = reader.inputStream()) {
                                serializableSnapshot = m_snapshotSerializer.deserializeSnapshot(storeName, is);
                            }
                            if (serializableSnapshot.getSnapshotModelVersion() != SNAPSHOT_MODEL_VERSION) {
                                throw new UnrecoverableStoreException("Snapshot serializable model version is not supported",
                                                                      args -> args.add("version", serializableSnapshot.getSnapshotModelVersion())
                                                                              .add("expectedVersion", SNAPSHOT_MODEL_VERSION));
                            }

                            if (!lastTx.equals(serializableSnapshot.getTransactionId())) {
                                throw new UnrecoverableStoreException("Snapshot transaction id  mismatch with request transaction id",
                                                                      args -> args.add("snapshotTransactionId", serializableSnapshot.getTransactionId())
                                                                              .add("requestTransactionId", lastTx));
                            }

                            serializableSnapshot.getEntities().forEach(serializableEntityInstances -> {
                                String entityName = serializableEntityInstances.getEntityName();
                                EntityStore<?> entityStore = entityStores.getEntityStore(entityName);
                                checkArgument(entityStore != null, "Entity has not be registered in the store", args -> args.add("entityName", entityName));

                                entityStore.recover(serializableEntityInstances);
                            });
                        });
            } catch (Exception e) {
                throw new UnrecoverableStoreException("Error occurred when recovering from latest snapshot", e);
            }
        });

        if (!latestSnapshotTxId.isPresent()) {
            LOG.info("Store has no snapshot, store is empty, creating it's first snapshot");
        }

        return latestSnapshotTxId;
    }

    void persistTransaction(String storeName, String synchronizationId, List<Command<?>> commands) {
        List<SerializableCommand> serializableCommands = commands.stream().map(m_commandProxyFactory::serializeCommand).collect(Collectors.toList());

        SerializableTransaction serializableTransaction = new SerializableTransaction();
        serializableTransaction.setTransactionModelVersion(TRANSACTION_MODEL_VERSION);
        serializableTransaction.setSynchronizationId(synchronizationId);
        serializableTransaction.setCommands(serializableCommands);

        try (OutputStream out = m_transactionStore.createTransactionOutput(storeName)) {
            LOG.debug("Enlisting new transaction", args -> args.add("Store", storeName).add("synchronizationId", synchronizationId));
            m_transactionSerializer.serializeTransaction(out, serializableTransaction);
        } catch (IOException e) {
            throw new StoreException(e);
        }
    }

    Flowable<TransactionToExecute> allTransactions() {
        return Flowable.fromPublisher(m_transactionStore.allTransactions()) //
                .map(this::createTransactionToExecute) //
                .doOnNext(tx -> LOG.debug("Received new transaction", args ->          //
                        args.add("Store", tx.m_storeName).add("synchronizationId", tx.m_synchronizationId).add("txId", tx.m_txId)));
    }

    @Nonnull
    private TransactionToExecute createTransactionToExecute(TransactionStore.TransactionInput transactionInput) throws IOException {
        SerializableTransaction serializableTransaction;
        try (InputStream in = transactionInput.getInputStream()) {
            serializableTransaction = m_transactionSerializer.deserializeTransaction(in);
        }

        if (serializableTransaction.getTransactionModelVersion() != TRANSACTION_MODEL_VERSION) {
            throw new StoreException("Transaction serializable model version is not supported",
                                     args -> args.add("version", serializableTransaction.getTransactionModelVersion())
                                             .add("expectedVersion", TRANSACTION_MODEL_VERSION));
        }

        List<Command<?>> commands = serializableTransaction.getCommands().stream().map(serializableCommand -> {
            String commandName = serializableCommand.getCommandName();
            CommandDefinition<? extends Command<?>> commandDefinition = m_commandDefinitions.get(commandName);
            checkArgument(commandDefinition != null, "Command has not been registered in the store", args -> args.add("commandName", commandName));

            Class<? extends Command<?>> commandClass = commandDefinition.getCommandClass();
            return m_commandProxyFactory.createCommand(commandClass, serializableCommand.getParameters());
        }).collect(Collectors.toList());

        return new TransactionToExecute(transactionInput.storeName(), transactionInput.getTransactionId(), serializableTransaction.getSynchronizationId(),
                                        commands);
    }

    static class TransactionToExecute {
        private final BigInteger m_txId;
        private final String m_synchronizationId;
        private final List<Command<?>> m_commands;
        private final String m_storeName;

        private TransactionToExecute(String storeName, BigInteger txId, String synchronizationId, List<Command<?>> commands) {
            m_storeName = storeName;
            m_txId = txId;
            m_synchronizationId = synchronizationId;
            m_commands = commands;
        }

        BigInteger getTxId() {
            return m_txId;
        }

        String getSynchronizationId() {
            return m_synchronizationId;
        }

        List<Command<?>> getCommands() {
            return m_commands;
        }

        String getStoreName() {
            return m_storeName;
        }
    }
}
