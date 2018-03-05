package io.axway.iron.sample.command;

import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.sample.model.Company;

public interface ChangeCompanyAddress extends Command<Void> {

    String name();

    @Nullable
    String newAddress();

    @Nullable
    String newCountry();

    @Override
    default Void execute(ReadWriteTransaction tx) {
        String newAddress = newAddress();
        String newCountry = newCountry();

        if (newAddress != null && newAddress.isEmpty()) {
            throw new IllegalArgumentException("New address must be null or not empty");
        }

        if (newCountry != null && newCountry.isEmpty()) {
            throw new IllegalArgumentException("New country must be null or not empty");
        }

        Company company = tx.select(Company.class).where(Company::name).equalsTo(name());
        tx.update(company) //
                .set(Company::address).to(newAddress) //
                .set(Company::country).to(newCountry) //
                .done();

        return null;
    }
}
