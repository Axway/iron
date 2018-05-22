package io.axway.iron.core.spi.file;

import java.nio.file.Path;
import java.util.function.*;
import javax.annotation.*;
import io.axway.iron.spi.storage.TransactionStore;

public class FileTransactionStoreBuilder implements Supplier<TransactionStore> {
    private Path m_dir;
    private Integer m_transactionIdPaddingLength = 20;
    private final String m_name;

    public FileTransactionStoreBuilder(String name) {
        m_name = name;
    }

    public FileTransactionStoreBuilder setDir(Path dir) {
        m_dir = dir.resolve(m_name);
        return this;
    }

    public FileTransactionStoreBuilder setTransactionIdPaddingLength(@Nullable Integer transactionIdPaddingLength) {
        m_transactionIdPaddingLength = transactionIdPaddingLength;
        return this;
    }

    @Override
    public TransactionStore get() {
        return new FileTransactionStore(m_dir, m_transactionIdPaddingLength);
    }
}
