package io.axway.iron.core.spi.file;

import java.nio.file.Path;
import java.util.function.*;
import javax.annotation.*;
import io.axway.iron.spi.storage.TransactionStoreFactory;

public class FileTransactionStoreFactoryBuilder implements Supplier<TransactionStoreFactory> {
    private Path m_dir;
    private Integer m_limitedSize = 20;

    public FileTransactionStoreFactoryBuilder setDir(Path dir) {
        m_dir = dir;
        return this;
    }

    public FileTransactionStoreFactoryBuilder setLimitedSize(@Nullable Integer limitedSize) {
        m_limitedSize = limitedSize;
        return this;
    }

    @Override
    public TransactionStoreFactory get() {
        return new FileStoreFactory(m_dir, m_limitedSize);
    }
}
