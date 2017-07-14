package io.axway.iron.core.store.relation.command;

import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.core.store.relation.model.Person;

public interface PersonDeleteCommand extends Command<Void> {

    String name();

    @Override
    default Void execute(@Nonnull ReadWriteTransaction tx) {
        Person person = tx.select(Person.class).where(Person::name).equalsTo(name());
        tx.delete(person);
        return null;
    }
}
