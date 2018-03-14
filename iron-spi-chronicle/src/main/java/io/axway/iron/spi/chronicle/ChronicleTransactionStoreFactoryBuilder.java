package io.axway.iron.spi.chronicle;

import java.nio.file.Path;
import java.util.function.*;
import io.axway.iron.spi.storage.TransactionStoreFactory;

public class ChronicleTransactionStoreFactoryBuilder implements Supplier<TransactionStoreFactory> {
    private Path m_dir;

    public ChronicleTransactionStoreFactoryBuilder setDir(Path dir) {
        m_dir = dir;
        return this;
    }

    @Override
    public TransactionStoreFactory get() {
        return new ChronicleTransactionStoreFactory(m_dir);
    }
}
