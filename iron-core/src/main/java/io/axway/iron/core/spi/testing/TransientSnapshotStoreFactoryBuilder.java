package io.axway.iron.core.spi.testing;

import java.util.function.*;
import io.axway.iron.spi.storage.SnapshotStoreFactory;

public class TransientSnapshotStoreFactoryBuilder implements Supplier<SnapshotStoreFactory> {
    @Override
    public SnapshotStoreFactory get() {
        return new TransientStoreFactory();
    }
}
