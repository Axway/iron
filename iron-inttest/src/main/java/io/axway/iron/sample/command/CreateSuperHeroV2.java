package io.axway.iron.sample.command;

import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.sample.model.SuperHeroV2;

public interface CreateSuperHeroV2 extends Command<SuperHeroV2> {

    String nickName();

    @Override
    default SuperHeroV2 execute(ReadWriteTransaction tx) {
        return tx.insert(SuperHeroV2.class).
                set(SuperHeroV2::nickName).to(nickName()).
                done();
    }
}
