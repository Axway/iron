package io.axway.iron.core.store.relation.command;

import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.core.store.relation.model.Person;

public interface PersonCreateCommand extends Command<Void> {

    String name();

    @Override
    default Void execute(@Nonnull ReadWriteTransaction tx) {
        tx.insert(Person.class).set(Person::name).to(name()).done();

        return null;
    }
}
