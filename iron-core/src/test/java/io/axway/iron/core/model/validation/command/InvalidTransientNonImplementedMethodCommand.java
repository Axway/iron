package io.axway.iron.core.model.validation.command;

import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.description.Transient;

public interface InvalidTransientNonImplementedMethodCommand extends Command<Void> {

    @Transient
    String value2();

    @Override
    default Void execute(@Nonnull ReadWriteTransaction tx) {
        return null;
    }
}
