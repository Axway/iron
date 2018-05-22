package io.axway.iron.core.store.datatype;

import java.util.*;
import io.axway.iron.ReadOnlyTransaction;
import io.axway.iron.Store;
import io.axway.iron.core.StoreManagerBuilder;
import io.axway.iron.core.store.SucceedingStoreTest;
import io.axway.iron.core.store.datatype.command.PrimitiveTypeInsertCommand;
import io.axway.iron.core.store.datatype.model.PrimitiveTypeEntity;

import static org.assertj.core.api.Assertions.assertThat;

class PrimitiveTypeInsertTest implements SucceedingStoreTest {

    @Override
    public void configure(StoreManagerBuilder builder) throws Exception {
        builder.withEntityClass(PrimitiveTypeEntity.class).withCommandClass(PrimitiveTypeInsertCommand.class);
    }

    @Override
    public void execute(Store store) throws Exception {
        store.createCommand(PrimitiveTypeInsertCommand.class) //
                .set(PrimitiveTypeInsertCommand::booleanValue).to(true) //
                .set(PrimitiveTypeInsertCommand::byteValue).to((byte) 2) //
                .set(PrimitiveTypeInsertCommand::charValue).to('c') //
                .set(PrimitiveTypeInsertCommand::shortValue).to((short) 4) //
                .set(PrimitiveTypeInsertCommand::intValue).to(5) //
                .set(PrimitiveTypeInsertCommand::longValue).to(6L) //
                .set(PrimitiveTypeInsertCommand::floatValue).to(7.7f) //
                .set(PrimitiveTypeInsertCommand::doubleValue).to(8.8d) //
                .submit().get();
    }

    @Override
    public void verify(ReadOnlyTransaction tx) {
        Collection<PrimitiveTypeEntity> list = tx.select(PrimitiveTypeEntity.class).all();
        assertThat(list.size()).isEqualTo(1);
        PrimitiveTypeEntity i = list.iterator().next();
        assertThat(i.booleanValue()).isEqualTo(true);
        assertThat(i.byteValue()).isEqualTo((byte) 2);
        assertThat(i.charValue()).isEqualTo('c');
        assertThat(i.shortValue()).isEqualTo((short) 4);
        assertThat(i.intValue()).isEqualTo(5);
        assertThat(i.longValue()).isEqualTo(6L);
        assertThat(i.floatValue()).isEqualTo(7.7f);
        assertThat(i.doubleValue()).isEqualTo(8.8d);
    }
}
