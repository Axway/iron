package io.axway.iron.core.bugs;

import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;

public interface SnapshotStoreCommand extends Command<Long> {

    String value();

    @Override
    default Long execute(ReadWriteTransaction tx) {
        SnapshotStoreEntityWithId createdObject = tx.insert(SnapshotStoreEntityWithId.class).set(SnapshotStoreEntityWithId::value).to(value()).done();
        return createdObject.id();
    }
}
