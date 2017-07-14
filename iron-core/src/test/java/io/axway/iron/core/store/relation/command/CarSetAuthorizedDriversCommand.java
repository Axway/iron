package io.axway.iron.core.store.relation.command;

import java.util.*;
import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.core.store.relation.model.Car;
import io.axway.iron.core.store.relation.model.Person;

public interface CarSetAuthorizedDriversCommand extends Command<Void> {
    String plateNumber();

    Collection<String> authorizedDrivers();

    @Override
    default Void execute(@Nonnull ReadWriteTransaction tx) {
        Car car = tx.select(Car.class).where(Car::plateNumber).equalsTo(plateNumber());
        Collection<Person> authorizedDrivers = tx.select(Person.class).where(Person::name).allContainedIn(authorizedDrivers());

        tx.update(car).set(Car::authorizedDrivers).to(authorizedDrivers).done();

        return null;
    }
}
