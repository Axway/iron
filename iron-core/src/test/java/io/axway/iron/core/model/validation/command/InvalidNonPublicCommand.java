package io.axway.iron.core.model.validation.command;

import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;

interface InvalidNonPublicCommand extends Command<Void> {
    @Override
    default Void execute(@Nonnull ReadWriteTransaction tx) {
        return null;
    }
}
