package io.axway.iron.sample.command;

import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.sample.model.Company;
import io.axway.iron.sample.model.Person;

public interface PersonRaiseSalary extends Command<Void> {

    String personId();

    double newSalary();

    @Override
    default Void execute(ReadWriteTransaction tx) {
        Person person = tx.select(Person.class).where(Person::id).equalsTo(personId());
        Company currentCompany = person.worksAt();
        Double salary = person.salary();

        if (currentCompany == null || salary == null) {
            throw new IllegalStateException("Cannot raise salary of an unemployed person {personId=" + person.id() + "}");
        }

        if (newSalary() < salary) {
            throw new IllegalStateException("New salary must but higher than current salary {currentSalary=" + salary + ", newSalary=" + newSalary() + "}");
        }

        tx.update(person) //
                .set(Person::salary).to(newSalary()) //
                .done();

        return null;
    }
}
