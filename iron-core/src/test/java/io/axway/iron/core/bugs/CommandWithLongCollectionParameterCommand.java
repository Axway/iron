package io.axway.iron.core.bugs;

import java.util.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;

public interface CommandWithLongCollectionParameterCommand extends Command<Long> {
    Collection<Long> longParameters();

    @Override
    default Long execute(ReadWriteTransaction tx) {
        Long result = null;
        for (Long l : longParameters()) {
            if (l != null) {
                if (result == null) {
                    result = 0L;
                }
                result += l;
            }
        }
        return result;
    }
}
