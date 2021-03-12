package io.axway.iron.sample.command;

import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.sample.model.SuperHeroV1;

public interface CreateSuperHeroV1 extends Command<SuperHeroV1> {

    String firstName();

    String lastName();

    @Override
    default SuperHeroV1 execute(ReadWriteTransaction tx) {
        return tx.insert(SuperHeroV1.class).
                set(SuperHeroV1::firstName).to(firstName()).//
                set(SuperHeroV1::lastName).to(lastName()).//
                done();
    }
}
