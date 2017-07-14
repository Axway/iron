package io.axway.iron.core.store.relation;

import io.axway.iron.ReadOnlyTransaction;
import io.axway.iron.Store;
import io.axway.iron.core.store.SucceedingStoreTest;
import io.axway.iron.core.store.relation.command.CarDeleteCommand;
import io.axway.iron.core.store.relation.model.Car;
import io.axway.iron.core.store.relation.model.Person;

import static org.assertj.core.api.Assertions.assertThat;

class ShouldDeleteRelationTailTest extends AbstractRelationTest implements SucceedingStoreTest {
    @Override
    public void execute(Store store) throws Exception {
        store.createCommand(CarDeleteCommand.class).set(CarDeleteCommand::plateNumber).to("XYZ").submit().get();
    }

    @Override
    public void verify(ReadOnlyTransaction tx) {
        Person john = tx.select(Person.class).where(Person::name).equalsTo("john");
        assertThat(john).isNotNull();

        Car xyz = tx.select(Car.class).where(Car::plateNumber).equalsToOrNull("XYZ");
        assertThat(xyz).isNull();

        assertThat(john.ownedCars()).isEmpty();
        assertThat(john.authorizedCars()).isEmpty();
    }
}
