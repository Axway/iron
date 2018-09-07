package io.axway.iron.sample.command;

import java.util.*;
import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.sample.model.Company;
import io.axway.iron.sample.model.Person;

public interface MultipleRelationsTestCommand extends Command<Void> {

    String personId();

    @Override
    default Void execute(ReadWriteTransaction tx) {
        Person person = tx.select(Person.class).where(Person::id).equalsTo(personId());
        Company previousCompany = person.worksAt();
        if (previousCompany == null) {
            throw new IllegalStateException("Unemployed person cannot leave a company {personId=" + person.id() + "}");
        }

        Company google    = tx.select(Company.class).where(Company::name).equalsToOrNull("Google");
        Company microsoft = tx.select(Company.class).where(Company::name).equalsToOrNull("Microsoft");
        Company oracle    = tx.select(Company.class).where(Company::name).equalsToOrNull("Oracle");

        tx.update(person) //
                //.onCollection(Person::previousCompanies)
                .onCollection(Person::previousCompanies).add(google)
                //.onCollection(Person::previousCompanies).add(microsoft)
                //.onCollection(Person::previousCompanies).add(previousCompany)
                .onCollection(Person::previousCompanies).add(previousCompany)
                .onCollection(Person::previousCompanies).addAll(Arrays.asList(microsoft, oracle))
                .remove(previousCompany)
                //.add(previousCompany)
                //.removeAll(Arrays.asList(oracle, previousCompany))
                //.removeAll(Arrays.asList(google, microsoft))
                //.clear().add(google).remove(previousCompany)
                //.remove(google)
                //.clear()
                .done();

        return null;
    }
}

