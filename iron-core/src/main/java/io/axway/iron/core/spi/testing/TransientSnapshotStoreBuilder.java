package io.axway.iron.core.spi.testing;

import java.util.function.*;
import io.axway.iron.spi.storage.SnapshotStore;

public class TransientSnapshotStoreBuilder implements Supplier<SnapshotStore> {
    @Override
    public SnapshotStore get() {
        return new TransientStore();
    }
}
