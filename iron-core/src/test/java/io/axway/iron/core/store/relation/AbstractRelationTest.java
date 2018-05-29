package io.axway.iron.core.store.relation;

import com.google.common.collect.ImmutableList;
import io.axway.iron.Store;
import io.axway.iron.core.StoreManagerBuilder;
import io.axway.iron.core.store.StoreTest;
import io.axway.iron.core.store.relation.command.CarChangeOwner;
import io.axway.iron.core.store.relation.command.CarCreateCommand;
import io.axway.iron.core.store.relation.command.CarDeleteCommand;
import io.axway.iron.core.store.relation.command.CarSetAuthorizedDriversCommand;
import io.axway.iron.core.store.relation.command.PersonCreateCommand;
import io.axway.iron.core.store.relation.command.PersonDeleteCommand;
import io.axway.iron.core.store.relation.model.Car;
import io.axway.iron.core.store.relation.model.Person;

abstract class AbstractRelationTest implements StoreTest {

    @Override
    public void configure(StoreManagerBuilder builder) throws Exception {
        builder //
                .withEntityClass(Car.class) //
                .withEntityClass(Person.class) //
                .withCommandClass(CarChangeOwner.class) //
                .withCommandClass(CarCreateCommand.class) //
                .withCommandClass(CarDeleteCommand.class) //
                .withCommandClass(CarSetAuthorizedDriversCommand.class) //
                .withCommandClass(PersonCreateCommand.class) //
                .withCommandClass(PersonDeleteCommand.class) //
        ;
    }

    @Override
    public void provision(Store store) throws Exception {
        Store.TransactionBuilder tx = store.begin();
        tx.addCommand(PersonCreateCommand.class).set(PersonCreateCommand::name).to("john").submit();
        tx.addCommand(PersonCreateCommand.class).set(PersonCreateCommand::name).to("marie").submit();
        tx.addCommand(PersonCreateCommand.class).set(PersonCreateCommand::name).to("george").submit();
        tx.addCommand(PersonCreateCommand.class).set(PersonCreateCommand::name).to("anna").submit();
        tx.addCommand(CarCreateCommand.class).set(CarCreateCommand::plateNumber).to("XYZ").set(CarCreateCommand::ownerName).to("john").submit();
        tx.addCommand(CarCreateCommand.class).set(CarCreateCommand::plateNumber).to("ABC").set(CarCreateCommand::ownerName).to("john").submit();
        tx.addCommand(CarCreateCommand.class).set(CarCreateCommand::plateNumber).to("123").set(CarCreateCommand::ownerName).to("george").submit();

        tx.addCommand(CarChangeOwner.class).set(CarChangeOwner::plateNumber).to("ABC").set(CarChangeOwner::newOwnerName).to("marie").submit();

        tx.addCommand(CarSetAuthorizedDriversCommand.class) //
                .set(CarSetAuthorizedDriversCommand::plateNumber).to("XYZ") //
                .set(CarSetAuthorizedDriversCommand::authorizedDrivers).to(ImmutableList.of("john", "marie", "anna")) //
                .submit();

        tx.addCommand(CarSetAuthorizedDriversCommand.class) //
                .set(CarSetAuthorizedDriversCommand::plateNumber).to("123") //
                .set(CarSetAuthorizedDriversCommand::authorizedDrivers).to(ImmutableList.of("marie", "anna")) //
                .submit();

        tx.addCommand(CarSetAuthorizedDriversCommand.class) //
                .set(CarSetAuthorizedDriversCommand::plateNumber).to("ABC") //
                .set(CarSetAuthorizedDriversCommand::authorizedDrivers).to(ImmutableList.of("marie")) //
                .submit();

        tx.submit().get();
    }
}
