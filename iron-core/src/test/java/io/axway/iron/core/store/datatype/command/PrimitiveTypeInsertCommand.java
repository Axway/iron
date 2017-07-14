package io.axway.iron.core.store.datatype.command;

import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.core.store.datatype.model.PrimitiveTypeEntity;

public interface PrimitiveTypeInsertCommand extends Command<Void> {
    boolean booleanValue();

    byte byteValue();

    char charValue();

    short shortValue();

    int intValue();

    long longValue();

    float floatValue();

    double doubleValue();

    @Override
    default Void execute(@Nonnull ReadWriteTransaction tx) {
        tx.insert(PrimitiveTypeEntity.class) //
                .set(PrimitiveTypeEntity::booleanValue).to(booleanValue()) //
                .set(PrimitiveTypeEntity::byteValue).to(byteValue()) //
                .set(PrimitiveTypeEntity::charValue).to(charValue()) //
                .set(PrimitiveTypeEntity::shortValue).to(shortValue()) //
                .set(PrimitiveTypeEntity::intValue).to(intValue()) //
                .set(PrimitiveTypeEntity::longValue).to(longValue()) //
                .set(PrimitiveTypeEntity::floatValue).to(floatValue()) //
                .set(PrimitiveTypeEntity::doubleValue).to(doubleValue()) //
                .done();

        return null;
    }
}
