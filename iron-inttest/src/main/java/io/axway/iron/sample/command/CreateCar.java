package io.axway.iron.sample.command;

import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.sample.model.Car;

public interface CreateCar extends Command<Void> {

    String name();

    @Override
    default Void execute(ReadWriteTransaction tx) {

        tx.insert(Car.class) //
                .set(Car::name).to(name()) //
                .done();

        return null;
    }
}
