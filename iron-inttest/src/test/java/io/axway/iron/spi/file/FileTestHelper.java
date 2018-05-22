package io.axway.iron.spi.file;

import java.nio.file.Path;
import io.axway.iron.core.spi.file.FileSnapshotStoreBuilder;
import io.axway.iron.core.spi.file.FileTransactionStoreBuilder;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

public class FileTestHelper {

    public static SnapshotStore buildFileSnapshotStore(Path filePath, String name) {
        return new FileSnapshotStoreBuilder(name).setDir(filePath).get();
    }

    public static TransactionStore buildFileTransactionStore(Path filePath, String name) {
        return new FileTransactionStoreBuilder(name).setDir(filePath).get();
    }

    public static SnapshotStore buildFileSnapshotStore(Path filePath, String name, Integer transactionIdPaddingLength) {
        return new FileSnapshotStoreBuilder(name).setDir(filePath).setTransactionIdLength(transactionIdPaddingLength).get();
    }

    public static TransactionStore buildFileTransactionStore(Path filePath, String name, Integer transactionIdPaddingLength) {
        return new FileTransactionStoreBuilder(name).setDir(filePath).setTransactionIdPaddingLength(transactionIdPaddingLength).get();
    }
}
