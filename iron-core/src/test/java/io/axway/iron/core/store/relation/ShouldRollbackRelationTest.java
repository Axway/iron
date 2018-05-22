package io.axway.iron.core.store.relation;

import java.util.*;
import javax.annotation.*;
import com.google.common.collect.ImmutableList;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.Store;
import io.axway.iron.core.StoreManagerBuilder;
import io.axway.iron.core.store.FailingStoreTest;
import io.axway.iron.core.store.relation.command.CarChangeOwner;
import io.axway.iron.core.store.relation.command.CarCreateCommand;
import io.axway.iron.core.store.relation.command.CarSetAuthorizedDriversCommand;
import io.axway.iron.core.store.relation.model.Car;
import io.axway.iron.core.store.relation.model.Person;
import io.axway.iron.error.StoreException;

class ShouldRollbackRelationTest extends AbstractRelationTest implements FailingStoreTest {
    @Override
    public void configure(StoreManagerBuilder builder) throws Exception {
        super.configure(builder);
        builder.withCommandClass(CarFailingCommand.class);
    }

    public interface CarFailingCommand extends Command<Void> {
        String plateNumber();

        Collection<String> authorizedDrivers();

        String previousOwner();

        @Override
        default Void execute(@Nonnull ReadWriteTransaction tx) {
            Car car = tx.select(Car.class).where(Car::plateNumber).equalsTo(plateNumber());
            Person previousOwner = tx.select(Person.class).where(Person::name).equalsTo(previousOwner());
            Collection<Person> authorizedDrivers = tx.select(Person.class).where(Person::name).allContainedIn(authorizedDrivers());

            tx.update(car) //
                    .set(Car::authorizedDrivers).to(authorizedDrivers) //
                    .set(Car::previousOwner).to(previousOwner) //
                    .done();

            throw new StoreException("Kaboom");
        }
    }

    @Override
    public void execute(Store store) throws Exception {
        store.createCommand(CarCreateCommand.class) //
                .set(CarCreateCommand::plateNumber).to("ZZZ") //
                .set(CarCreateCommand::ownerName).to("john") //
                .submit();

        store.createCommand(CarSetAuthorizedDriversCommand.class) //
                .set(CarSetAuthorizedDriversCommand::plateNumber).to("ZZZ") //
                .set(CarSetAuthorizedDriversCommand::authorizedDrivers).to(ImmutableList.of("john")) //
                .submit();

        store.createCommand(CarChangeOwner.class) //
                .set(CarChangeOwner::plateNumber).to("ZZZ") //
                .set(CarChangeOwner::newOwnerName).to("marie") //
                .submit();

        store.createCommand(CarFailingCommand.class) //
                .set(CarFailingCommand::plateNumber).to("ZZZ") //
                .set(CarFailingCommand::authorizedDrivers).to(ImmutableList.of("marie")) //
                .set(CarFailingCommand::previousOwner).to("anna") //
                .submit().get();
    }
}
