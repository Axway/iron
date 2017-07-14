package io.axway.iron.sample.command;

import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.sample.model.Company;

public interface CreateCompany extends Command<Long> {

    String name();

    @Nullable
    String address();

    @Override
    default Long execute(ReadWriteTransaction tx) {
        String address = address();
        if (address != null && address.isEmpty()) {
            throw new IllegalArgumentException("New address must be null or not empty");
        }

        Company company = tx.insert(Company.class) //
                .set(Company::name).to(name()) //
                .set(Company::address).to(address) //
                .done();

        return company.id();
    }
}
