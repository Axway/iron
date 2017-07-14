package io.axway.iron.core.model.validation.command;

import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.description.Unique;

public interface InvalidMethodUniqueCommand extends Command<Void> {
    @Unique
    String value();

    @Override
    default Void execute(@Nonnull ReadWriteTransaction tx) {
        return null;
    }
}
