package io.axway.iron.sample.command;

import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.sample.model.Company;
import io.axway.iron.sample.model.Person;

public interface MultipleRelationsAddOneTestCommand extends Command<Void> {

    String personId();

    @Override
    default Void execute(ReadWriteTransaction tx) {
        Person person = tx.select(Person.class).where(Person::id).equalsTo(personId());

        Company google = tx.select(Company.class).where(Company::name).equalsToOrNull("Google");
        Company microsoft = tx.select(Company.class).where(Company::name).equalsToOrNull("Microsoft");

        tx.update(person) //
                .onCollection(Person::previousCompanies).add(google).add(microsoft) //
                .done();

        return null;
    }
}

