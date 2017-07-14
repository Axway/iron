package io.axway.iron.core.bugs;

import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;

public interface CommandWithLongParameterCommand extends Command<Long> {
    @Nullable
    Long longParameter();

    @Override
    default Long execute(ReadWriteTransaction tx) {
        return longParameter();
    }
}
