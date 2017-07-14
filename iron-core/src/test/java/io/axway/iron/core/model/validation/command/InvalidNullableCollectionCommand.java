package io.axway.iron.core.model.validation.command;

import java.util.*;
import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;

public interface InvalidNullableCollectionCommand extends Command<Void> {

    @Nullable
    Collection<String> ids();

    @Override
    default Void execute(@Nonnull ReadWriteTransaction tx) {
        return null;
    }
}
