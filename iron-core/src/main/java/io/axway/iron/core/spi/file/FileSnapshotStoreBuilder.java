package io.axway.iron.core.spi.file;

import java.nio.file.Path;
import java.util.function.*;
import javax.annotation.*;
import io.axway.iron.spi.storage.SnapshotStore;

public class FileSnapshotStoreBuilder implements Supplier<SnapshotStore> {
    private Path m_dir;
    private Integer m_transactionIdLength = 20;
    private final String m_name;

    public FileSnapshotStoreBuilder(String name) {
        m_name = name;
    }

    public FileSnapshotStoreBuilder setDir(Path dir) {
        m_dir = dir.resolve(m_name);
        return this;
    }

    public FileSnapshotStoreBuilder setTransactionIdLength(@Nullable Integer transactionIdLength) {
        m_transactionIdLength = transactionIdLength;
        return this;
    }

    @Override
    public SnapshotStore get() {
        return new FileSnapshotStore(m_dir, m_transactionIdLength);
    }
}
