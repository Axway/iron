package io.axway.iron.core.store.relation;

import io.axway.iron.ReadOnlyTransaction;
import io.axway.iron.Store;
import io.axway.iron.core.store.SucceedingStoreTest;
import io.axway.iron.core.store.relation.command.CarChangeOwner;
import io.axway.iron.core.store.relation.model.Car;
import io.axway.iron.core.store.relation.model.Person;

import static org.assertj.core.api.Assertions.assertThat;

class ShouldUpdateSimpleRelationHeadTest extends AbstractRelationTest implements SucceedingStoreTest {
    @Override
    public void execute(Store store) throws Exception {
        store.createCommand(CarChangeOwner.class).set(CarChangeOwner::plateNumber).to("123").set(CarChangeOwner::newOwnerName).to("marie").submit().get();
    }

    @Override
    public void verify(ReadOnlyTransaction tx) {
        Person george = tx.select(Person.class).where(Person::name).equalsTo("george");
        assertThat(george).isNotNull();

        Person marie = tx.select(Person.class).where(Person::name).equalsTo("marie");
        assertThat(marie).isNotNull();

        Car c123 = tx.select(Car.class).where(Car::plateNumber).equalsTo("123");
        assertThat(c123).isNotNull();
        assertThat(c123.owner()).isEqualTo(marie);
        assertThat(c123.previousOwner()).isEqualTo(george);

        assertThat(george.ownedCars()).isEmpty();

        Car abc = tx.select(Car.class).where(Car::plateNumber).equalsTo("ABC");
        assertThat(marie.ownedCars()).containsExactlyInAnyOrder(c123, abc);
    }
}
