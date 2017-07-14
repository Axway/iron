package io.axway.iron.core.model.validation.command;

import io.axway.iron.ReadWriteTransaction;

public interface InvalidNonExtendingCommandCommand {
    default Void execute(ReadWriteTransaction tx) {
        return null;
    }
}
