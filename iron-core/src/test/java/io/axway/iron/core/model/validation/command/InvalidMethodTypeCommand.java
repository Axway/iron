package io.axway.iron.core.model.validation.command;

import java.lang.reflect.Method;
import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;

public interface InvalidMethodTypeCommand extends Command<Void> {
    Method value();

    @Override
    default Void execute(@Nonnull ReadWriteTransaction tx) {
        return null;
    }
}
