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

    public static SnapshotStoreFactory buildFileSnapshotStoreFactory(Path filePath, Integer limitedSize) {
        return new FileSnapshotStoreFactoryBuilder().setDir(filePath).setLimitedSize(limitedSize).get();
    }

    public static TransactionStoreFactory buildFileTransactionStoreFactory(Path filePath, Integer limitedSize) {
        return new FileTransactionStoreFactoryBuilder().setDir(filePath).setLimitedSize(limitedSize).get();
    }
}
