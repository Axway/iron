package io.axway.iron.core.internal;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.ImmutableMap;
import io.axway.iron.Command;
import io.axway.iron.core.internal.command.CommandProxyFactory;
import io.axway.iron.core.internal.definition.command.CommandDefinition;
import io.axway.iron.core.internal.entity.EntityStore;
import io.axway.iron.error.StoreException;
import io.axway.iron.spi.model.snapshot.SerializableSnapshot;
import io.axway.iron.spi.model.transaction.SerializableCommand;
import io.axway.iron.spi.model.transaction.SerializableTransaction;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

import static com.google.common.base.Preconditions.checkArgument;
import static io.axway.iron.spi.model.snapshot.SerializableSnapshot.SNAPSHOT_MODEL_VERSION;
import static io.axway.iron.spi.model.transaction.SerializableTransaction.TRANSACTION_MODEL_VERSION;
import static java.util.concurrent.TimeUnit.*;

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

    void persistSnapshot(BigInteger txId, Collection<EntityStore<?>> entityStores) {
        SerializableSnapshot serializableSnapshot = new SerializableSnapshot();
        serializableSnapshot.setSnapshotModelVersion(SNAPSHOT_MODEL_VERSION);
        serializableSnapshot.setTransactionId(txId);
        serializableSnapshot.setEntities(entityStores.stream().map(EntityStore::snapshot).collect(Collectors.toList()));

        try (OutputStream out = m_snapshotStore.createSnapshotWriter(txId)) {
            m_snapshotSerializer.serializeSnapshot(out, serializableSnapshot);
        } catch (IOException e) {
            throw new UncheckedIOException("Error when creating the store snapshot for transaction " + txId, e);
        }
    }

    BigInteger recover(Function<String, EntityStore<?>> entityStoreByName) {
        Optional<BigInteger> latestSnapshotTxId = m_snapshotStore.listSnapshots().stream().max(BigInteger::compareTo);
        if (latestSnapshotTxId.isPresent()) {
            BigInteger latestTxId = latestSnapshotTxId.get();
            LOG.info("Recovering store from snapshot {transactionId={}}", latestTxId);

            SerializableSnapshot serializableSnapshot;
            try (InputStream in = m_snapshotStore.createSnapshotReader(latestTxId)) {
                serializableSnapshot = m_snapshotSerializer.deserializeSnapshot(in);
            } catch (IOException e) {
                throw new UncheckedIOException("Error occurred when recovering from latest snapshot", e);
            }

            if (serializableSnapshot.getSnapshotModelVersion() != SNAPSHOT_MODEL_VERSION) {
                throw new IllegalStateException(
                        "Snapshot serializable model version " + serializableSnapshot.getSnapshotModelVersion() + " is not supported. Version "
                                + SNAPSHOT_MODEL_VERSION + " is expected");
            }

            if (latestTxId.compareTo(serializableSnapshot.getTransactionId()) != 0) {
                throw new IllegalStateException(
                        "Snapshot transaction id " + serializableSnapshot.getTransactionId() + " mismatch with request transaction id " + latestTxId);
            }

            serializableSnapshot.getEntities().forEach(serializableEntityInstances -> {
                String entityName = serializableEntityInstances.getEntityName();
                EntityStore<?> entityStore = entityStoreByName.apply(entityName);
                checkArgument(entityStore != null, "Entity name %s has not be registered in the store", entityName);

                entityStore.recover(serializableEntityInstances);
            });

            m_transactionStore.seekTransactionPoll(latestTxId);
            return latestTxId;
        } else {
            LOG.info("Store has no snapshot, store is empty, creating it's first snapshot");
            return null;
        }
    }

    void persistTransaction(String synchronizationId, List<Command<?>> commands) {
        List<SerializableCommand> serializableCommands = commands.stream().map(m_commandProxyFactory::serializeCommand).collect(Collectors.toList());

        SerializableTransaction serializableTransaction = new SerializableTransaction();
        serializableTransaction.setTransactionModelVersion(TRANSACTION_MODEL_VERSION);
        serializableTransaction.setSynchronizationId(synchronizationId);
        serializableTransaction.setCommands(serializableCommands);

        try (OutputStream out = m_transactionStore.createTransactionOutput()) {
            m_transactionSerializer.serializeTransaction(out, serializableTransaction);
        } catch (IOException e) {
            throw new StoreException(e);
        }
    }

    TransactionToExecute pollNextTransaction(int timeoutMs) {
        TransactionStore.TransactionInput transactionInput = m_transactionStore.pollNextTransaction(timeoutMs, MILLISECONDS);
        if (transactionInput != null) {
            SerializableTransaction serializableTransaction;
            try (InputStream in = transactionInput.getInputStream()) {
                serializableTransaction = m_transactionSerializer.deserializeTransaction(in);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            if (serializableTransaction.getTransactionModelVersion() != TRANSACTION_MODEL_VERSION) {
                throw new IllegalStateException(
                        "Transaction serializable model version " + serializableTransaction.getTransactionModelVersion() + " is not supported. Version "
                                + TRANSACTION_MODEL_VERSION + " is expected");
            }

            List<Command<?>> commands = serializableTransaction.getCommands().stream().map(serializableCommand -> {
                String commandName = serializableCommand.getCommandName();
                CommandDefinition<? extends Command<?>> commandDefinition = m_commandDefinitions.get(commandName);
                checkArgument(commandDefinition != null, "Command name %s has not be registered in the store", commandName);

                Class<? extends Command<?>> commandClass = commandDefinition.getCommandClass();
                return m_commandProxyFactory.createCommand(commandClass, serializableCommand.getParameters());
            }).collect(Collectors.toList());

            return new TransactionToExecute(transactionInput.getTransactionId(), serializableTransaction.getSynchronizationId(), commands);
        } else {
            return null;
        }
    }

    static class TransactionToExecute {
        private final BigInteger m_txId;
        private final String m_synchronizationId;
        private final List<Command<?>> m_commands;

        private TransactionToExecute(BigInteger txId, String synchronizationId, List<Command<?>> commands) {
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
    }
}
