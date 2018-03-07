package io.axway.iron.sample.command;

import java.util.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.sample.model.Company;
import io.axway.iron.sample.model.Person;

public interface PersonSetPreviousCompanyHistory extends Command<Void> {

    String personId();

    Collection<String> companyNames();

    @Override
    default Void execute(ReadWriteTransaction tx) {
        Collection<Company> companies = tx.select(Company.class).where(Company::name).allContainedIn(companyNames());

        Person person = tx.select(Person.class).where(Person::id).equalsTo(personId());
        tx.update(person) //
                .set(Person::previousCompanies).to(companies) //
                .done();

        return null;
    }
}
