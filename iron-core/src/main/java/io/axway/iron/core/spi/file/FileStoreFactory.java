package io.axway.iron.core.spi.file;

import java.io.*;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStore;
import io.axway.iron.spi.storage.TransactionStoreFactory;

public class FileStoreFactory implements SnapshotStoreFactory, TransactionStoreFactory {
    private final File m_fileStoreDir;

    public FileStoreFactory(File fileStoreDir) {
        m_fileStoreDir = Util.ensureDirectoryExists(fileStoreDir);
    }

    @Override
    public SnapshotStore createSnapshotStore(String storeName) {
        // store name is already enforced to be securely usable for file name thanks to io.axway.iron.StoreManagerFactory.STORE_NAME_VALIDATOR_PATTERN
        return new FileSnapshotStore(new File(new File(m_fileStoreDir, storeName), "snapshot"));
    }

    @Override
    public TransactionStore createTransactionStore(String storeName) {
        // store name is already enforced to be securely usable for file name thanks to io.axway.iron.StoreManagerFactory.STORE_NAME_VALIDATOR_PATTERN
        return new FileTransactionStore(new File(new File(m_fileStoreDir, storeName), "tx"));
    }
}
