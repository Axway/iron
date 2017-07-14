package io.axway.iron.core.model.validation.command;

import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;

public interface InvalidMissingExecuteMethodImplementationCommand extends Command<Void> {
    @Override
    Void execute(@Nonnull ReadWriteTransaction tx);
}
