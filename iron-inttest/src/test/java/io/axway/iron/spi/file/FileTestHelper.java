package io.axway.iron.spi.file;

import java.nio.file.Path;
import io.axway.iron.core.spi.file.FileSnapshotStoreFactoryBuilder;
import io.axway.iron.core.spi.file.FileTransactionStoreFactoryBuilder;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStoreFactory;

public class FileTestHelper {

    public static SnapshotStoreFactory buildFileSnapshotStoreFactory(Path filePath) {
        return new FileSnapshotStoreFactoryBuilder().setDir(filePath).get();
    }

    public static TransactionStoreFactory buildFileTransactionStoreFactory(Path filePath) {
        return new FileTransactionStoreFactoryBuilder().setDir(filePath).get();
    }

    public static SnapshotStoreFactory buildFileSnapshotStoreFactory(Path filePath, Integer transactionIdPaddingLength) {
        return new FileSnapshotStoreFactoryBuilder().setDir(filePath).setTransactionIdPaddingLength(transactionIdPaddingLength).get();
    }

    public static TransactionStoreFactory buildFileTransactionStoreFactory(Path filePath, Integer transactionIdPaddingLength) {
        return new FileTransactionStoreFactoryBuilder().setDir(filePath).setTransactionIdPaddingLength(transactionIdPaddingLength).get();
    }
}
