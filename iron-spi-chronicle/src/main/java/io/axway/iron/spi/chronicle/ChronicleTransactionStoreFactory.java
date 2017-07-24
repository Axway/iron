package io.axway.iron.spi.chronicle;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import io.axway.iron.spi.storage.TransactionStore;
import io.axway.iron.spi.storage.TransactionStoreFactory;

public class ChronicleTransactionStoreFactory implements TransactionStoreFactory {
    private final Path m_chronicleStoreDir;

    public ChronicleTransactionStoreFactory(Path chronicleStoreDir) {
        m_chronicleStoreDir = ensureDirectoryExists(chronicleStoreDir);
    }

    @Override
    public TransactionStore createTransactionStore(String storeName) {
        Path storeDir = ensureDirectoryExists(m_chronicleStoreDir.resolve(storeName).resolve("tx"));
        return new ChronicleTransactionStore(storeDir);
    }

    private Path ensureDirectoryExists(Path dir) {
        try {
            return Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
