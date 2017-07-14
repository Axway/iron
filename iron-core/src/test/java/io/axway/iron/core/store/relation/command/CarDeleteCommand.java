package io.axway.iron.core.store.relation.command;

import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.core.store.relation.model.Car;

public interface CarDeleteCommand extends Command<Void> {

    String plateNumber();

    @Override
    default Void execute(@Nonnull ReadWriteTransaction tx) {
        Car car = tx.select(Car.class).where(Car::plateNumber).equalsTo(plateNumber());
        tx.delete(car);

        return null;
    }
}
