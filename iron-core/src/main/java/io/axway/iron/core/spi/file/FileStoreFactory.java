package io.axway.iron.core.spi.file;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.*;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStore;
import io.axway.iron.spi.storage.TransactionStoreFactory;

public class FileStoreFactory implements SnapshotStoreFactory, TransactionStoreFactory {
    private static final int DEFAULT_LIMITED_SIZE = 20;
    private final Path m_fileStoreDir;
    @Nullable
    private Integer m_limitedSize;

    public FileStoreFactory(Path fileStoreDir) {
        this(fileStoreDir, DEFAULT_LIMITED_SIZE);
    }

    public FileStoreFactory(Path fileStoreDir, @Nullable Integer limitedSize) {
        m_fileStoreDir = ensureDirectoryExists(fileStoreDir);
        m_limitedSize = limitedSize;
    }

    @Override
    public SnapshotStore createSnapshotStore(String storeName) {
        // store name is already enforced to be securely usable for file name thanks to io.axway.iron.StoreManagerFactory.STORE_NAME_VALIDATOR_PATTERN
        Path snapshotStoreDir = ensureDirectoryExists(m_fileStoreDir.resolve(storeName).resolve("snapshot"));
        Path snapshotStoreTmpDir = ensureDirectoryExists(snapshotStoreDir.resolve(".tmp"));
        return new FileSnapshotStore(snapshotStoreDir, snapshotStoreTmpDir, m_limitedSize);
    }

    @Override
    public TransactionStore createTransactionStore(String storeName) {
        // store name is already enforced to be securely usable for file name thanks to io.axway.iron.StoreManagerFactory.STORE_NAME_VALIDATOR_PATTERN
        Path transactionStoreDir = ensureDirectoryExists(m_fileStoreDir.resolve(storeName).resolve("tx"));
        Path transactionStoreTmpDir = ensureDirectoryExists(transactionStoreDir.resolve(".tmp"));
        return new FileTransactionStore(transactionStoreDir, transactionStoreTmpDir, m_limitedSize);
    }

    private Path ensureDirectoryExists(Path dir) {
        try {
            return Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
