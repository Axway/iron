package io.axway.iron.core.model.validation.command;

import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.description.Id;

public interface InvalidMethodIdCommand extends Command<Void> {
    @Id
    long value();

    @Override
    default Void execute(@Nonnull ReadWriteTransaction tx) {
        return null;
    }
}
