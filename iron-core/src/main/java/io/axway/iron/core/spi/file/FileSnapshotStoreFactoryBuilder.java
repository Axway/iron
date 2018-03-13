package io.axway.iron.core.spi.file;

import java.nio.file.Path;
import java.util.function.*;
import javax.annotation.*;
import io.axway.iron.spi.storage.SnapshotStoreFactory;

public class FileSnapshotStoreFactoryBuilder implements Supplier<SnapshotStoreFactory> {
    private Path m_dir;
    private Integer m_limitedSize = 20;

    public FileSnapshotStoreFactoryBuilder setDir(Path dir) {
        m_dir = dir;
        return this;
    }

    public FileSnapshotStoreFactoryBuilder setLimitedSize(@Nullable Integer limitedSize) {
        m_limitedSize = limitedSize;
        return this;
    }

    @Override
    public SnapshotStoreFactory get() {
        return new FileStoreFactory(m_dir, m_limitedSize);
    }
}
