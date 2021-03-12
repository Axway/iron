package io.axway.iron.core.model.simple;

import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;

public interface CreateSimpleEntity extends Command<SimpleEntity> {
    String id();

    @Override
    default SimpleEntity execute(ReadWriteTransaction tx) {
        return tx.insert(SimpleEntity.class).set(SimpleEntity::id).to(id()).done();
    }
}
