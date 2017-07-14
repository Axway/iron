package io.axway.iron.spi.chronicle;

import java.io.*;
import io.axway.iron.spi.storage.TransactionStore;
import io.axway.iron.spi.storage.TransactionStoreFactory;

public class ChronicleTransactionStoreFactory implements TransactionStoreFactory {
    private final File m_chronicleStoreDir;

    public ChronicleTransactionStoreFactory(File chronicleStoreDir) {
        m_chronicleStoreDir = Util.ensureDirectoryExists(chronicleStoreDir);
    }

    @Override
    public TransactionStore createTransactionStore(String storeName) {
        return new ChronicleTransactionStore(new File(new File(m_chronicleStoreDir, storeName), "tx"));
    }
}
