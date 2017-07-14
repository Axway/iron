package io.axway.iron.core.store.relation.command;

import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.core.store.relation.model.Car;
import io.axway.iron.core.store.relation.model.Person;

public interface CarChangeOwner extends Command<Void> {
    String plateNumber();

    String newOwnerName();

    @Override
    default Void execute(@Nonnull ReadWriteTransaction tx) {
        Car car = tx.select(Car.class).where(Car::plateNumber).equalsTo(plateNumber());
        Person newOwner = tx.select(Person.class).where(Person::name).equalsTo(newOwnerName());

        tx.update(car) //
                .set(Car::previousOwner).to(car.owner()) //
                .set(Car::owner).to(newOwner) //
                .done();

        return null;
    }
}
