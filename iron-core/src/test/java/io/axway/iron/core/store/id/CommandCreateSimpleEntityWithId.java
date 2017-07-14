package io.axway.iron.core.store.id;

import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;

public interface CommandCreateSimpleEntityWithId extends Command<Long> {

    String value();

    @Override
    default Long execute(ReadWriteTransaction tx) {
        SimpleEntityWithId createdObject = tx.insert(SimpleEntityWithId.class).set(SimpleEntityWithId::value).to(value()).done();
        return createdObject.id();
    }
}
