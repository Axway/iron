package io.axway.iron.core.spi.file;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStore;
import io.axway.iron.spi.storage.TransactionStoreFactory;

public class FileStoreFactory implements SnapshotStoreFactory, TransactionStoreFactory {
    private final Path m_fileStoreDir;

    public FileStoreFactory(Path fileStoreDir) {
        m_fileStoreDir = ensureDirectoryExists(fileStoreDir);
    }

    @Override
    public SnapshotStore createSnapshotStore(String storeName) {
        // store name is already enforced to be securely usable for file name thanks to io.axway.iron.StoreManagerFactory.STORE_NAME_VALIDATOR_PATTERN
        Path snapshotStoreDir = ensureDirectoryExists(m_fileStoreDir.resolve(storeName).resolve("snapshot"));
        Path snapshotStoreTmpDir = ensureDirectoryExists(snapshotStoreDir.resolve(".tmp"));
        return new FileSnapshotStore(snapshotStoreDir, snapshotStoreTmpDir);
    }

    @Override
    public TransactionStore createTransactionStore(String storeName) {
        // store name is already enforced to be securely usable for file name thanks to io.axway.iron.StoreManagerFactory.STORE_NAME_VALIDATOR_PATTERN
        Path transactionStoreDir = ensureDirectoryExists(m_fileStoreDir.resolve(storeName).resolve("tx"));
        Path transactionStoreTmpDir = ensureDirectoryExists(transactionStoreDir.resolve(".tmp"));
        return new FileTransactionStore(transactionStoreDir, transactionStoreTmpDir);
    }

    private Path ensureDirectoryExists(Path dir) {
        try {
            return Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
