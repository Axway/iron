package io.axway.iron.core.model.validation.command;

import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;

public interface InvalidMethodWithExceptionsCommand extends Command<Void> {
    String value() throws RuntimeException;

    @Override
    default Void execute(@Nonnull ReadWriteTransaction tx) {
        return null;
    }
}
