package io.axway.iron.sample.command;

import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.sample.model.Company;
import io.axway.iron.sample.model.Person;

public interface PersonLeaveCompany extends Command<Void> {

    String personId();

    @Override
    default Void execute(ReadWriteTransaction tx) {
        Person person = tx.select(Person.class).where(Person::id).equalsTo(personId());
        Company previousCompany = person.worksAt();
        if (previousCompany == null) {
            throw new IllegalStateException("Unemployed person cannot leave a company {personId=" + person.id() + "}");
        }

        tx.update(person) //
                .onCollection(Person::previousCompanies).add(previousCompany) //
                .set(Person::worksAt).to(null) //
                .set(Person::salary).to(null) //
                .done();

        return null;
    }
}
