package io.axway.iron.sample.command;

import java.util.*;
import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.sample.model.Company;
import io.axway.iron.sample.model.Person;

public interface CreatePerson extends Command<Void> {

    String id();

    String name();

    @Nullable
    Date birthDate();

    @Nullable
    String worksAt();

    Collection<String> previousCompanyNames();

    @Override
    default Void execute(ReadWriteTransaction tx) {
        Collection<String> previousCompanyNames = previousCompanyNames();
        if (previousCompanyNames == null) { //TODO command proxy should not return null Collection
            previousCompanyNames = Collections.emptyList();
        }
        Collection<Company> previousCompanies = tx.select(Company.class).where(Company::name).allContainedIn(previousCompanyNames);
        Company worksAt = tx.select(Company.class).where(Company::name).equalsToOrNull(worksAt());

        tx.insert(Person.class) //
                .set(Person::name).to(name()) //
                .set(Person::id).to(id()) //
                .set(Person::birthDate).to(birthDate()) //
                .set(Person::worksAt).to(worksAt) //
                .set(Person::previousCompanies).to(previousCompanies) //
                .done();

        return null;
    }
}
