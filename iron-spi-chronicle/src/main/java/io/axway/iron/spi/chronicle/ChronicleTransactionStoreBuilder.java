package io.axway.iron.spi.chronicle;

import java.nio.file.Path;
import java.util.function.*;
import io.axway.iron.spi.storage.TransactionStore;

public class ChronicleTransactionStoreBuilder implements Supplier<TransactionStore> {
    private Path m_dir;

    private final String m_name;

    public ChronicleTransactionStoreBuilder(String name) {
        m_name = name;
    }

    public ChronicleTransactionStoreBuilder setDir(Path dir) {
        m_dir = dir.resolve(m_name);
        return this;
    }

    @Override
    public TransactionStore get() {
        return new ChronicleTransactionStore(m_dir);
    }
}
