package io.axway.iron.core.model.validation.command;

import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;

public interface InvalidNonnullAndNullableMethodCommand extends Command<Void> {
    @Nullable
    @Nonnull
    String value();

    @Override
    default Void execute(@Nonnull ReadWriteTransaction tx) {
        return null;
    }
}
