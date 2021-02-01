package io.axway.iron.core.internal.command.management;

import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;

public interface MaintenanceModeCommand extends Command<Void> {
    @Override
    default Void execute(ReadWriteTransaction tx) {
        return null;
    }
}
