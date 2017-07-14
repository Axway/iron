package io.axway.iron.sample.command;

import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.sample.model.Company;

public interface DeleteCompany extends Command<Void> {

    String name();

    @Override
    default Void execute(ReadWriteTransaction tx) {
        if ("Google".equals(name())) {
            throw new IllegalArgumentException("You cannot delete Google!");
        }

        Company company = tx.select(Company.class).where(Company::name).equalsToOrNull(name());
        if (company != null) {
            tx.delete(company);
        }

        return null;
    }
}
