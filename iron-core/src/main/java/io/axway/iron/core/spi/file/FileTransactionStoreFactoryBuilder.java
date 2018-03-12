package io.axway.iron.core.spi.file;

import java.nio.file.Path;
import java.util.function.*;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static io.axway.iron.core.spi.file.FileStoreFactory.DEFAULT_TRANSACTION_ID_LENGTH;

public class FileTransactionStoreFactoryBuilder implements Supplier<TransactionStoreFactory> {
    private Path m_dir;
    private Integer m_limitedSize = DEFAULT_TRANSACTION_ID_LENGTH;

    public void setDir(Path dir) {
        m_dir = dir;
    }

    public void setLimitedSize(Integer limitedSize) {
        m_limitedSize = limitedSize;
    }

    @Override
    public TransactionStoreFactory get() {
        return new FileStoreFactory(m_dir, m_limitedSize);
    }
}
