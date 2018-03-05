package io.axway.iron.sample.command;

import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.sample.model.Company;
import io.axway.iron.sample.model.Person;

import static java.lang.String.format;

public interface PersonJoinCompany extends Command<Void> {

    String personId();

    String companyName();

    double salary();

    @Override
    default Void execute(ReadWriteTransaction tx) {
        Person person = tx.select(Person.class).where(Person::id).equalsTo(personId());
        Company currentCompany = person.worksAt();
        if (currentCompany != null) {
            throw new IllegalStateException(format("%s is already working at %s, cannot change it's company", person.id(), currentCompany.name()));
        }

        Company company = tx.select(Company.class).where(Company::name).equalsTo(companyName());
        tx.update(person) //
                .set(Person::worksAt).to(company) //
                .set(Person::salary).to(salary()) //
                .done();

        return null;
    }
}
