package io.axway.iron.core.model.simple;

import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;

public interface SimpleCommand extends Command<Void> {

    @Override
    default Void execute(ReadWriteTransaction tx) {
        return null;
    }
}
