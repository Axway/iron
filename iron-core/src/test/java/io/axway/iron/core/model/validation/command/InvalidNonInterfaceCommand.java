package io.axway.iron.core.model.validation.command;

import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;

public class InvalidNonInterfaceCommand implements Command {
    @Override
    public Void execute(@Nonnull ReadWriteTransaction tx) {
        return null;
    }
}
