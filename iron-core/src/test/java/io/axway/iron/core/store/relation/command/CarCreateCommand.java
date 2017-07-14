package io.axway.iron.core.store.relation.command;

import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.core.store.relation.model.Car;
import io.axway.iron.core.store.relation.model.Person;

public interface CarCreateCommand extends Command<Void> {

    String plateNumber();

    String ownerName();

    @Override
    default Void execute(@Nonnull ReadWriteTransaction tx) {
        Person owner = tx.select(Person.class).where(Person::name).equalsTo(ownerName());

        tx.insert(Car.class) //
                .set(Car::plateNumber).to(plateNumber()) //
                .set(Car::owner).to(owner) //
                .done();

        return null;
    }
}
