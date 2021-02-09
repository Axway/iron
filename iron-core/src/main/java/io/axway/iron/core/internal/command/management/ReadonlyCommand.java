package io.axway.iron.core.internal.command.management;

import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;

public interface ReadonlyCommand extends Command<Void> {
    boolean value();

    @Override
    default Void execute(ReadWriteTransaction tx) {
        return null;
    }
}
