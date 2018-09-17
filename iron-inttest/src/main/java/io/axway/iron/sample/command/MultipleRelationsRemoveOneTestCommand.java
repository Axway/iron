package io.axway.iron.sample.command;

import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.sample.model.Company;
import io.axway.iron.sample.model.Person;

public interface MultipleRelationsRemoveOneTestCommand extends Command<Void> {

    String personId();

    @Override
    default Void execute(ReadWriteTransaction tx) {
        Person person = tx.select(Person.class).where(Person::id).equalsTo(personId());

        Company currentCompany = person.worksAt();
        Company google = tx.select(Company.class).where(Company::name).equalsToOrNull("Google");
        Company oracle = tx.select(Company.class).where(Company::name).equalsToOrNull("Oracle");

        tx.update(person) //
                .onCollection(Person::previousCompanies).remove(oracle) //
                .remove(currentCompany) //
                .remove(google) //
                .done();

        return null;
    }
}

