package io.axway.iron.core.store.id;

import io.axway.iron.ReadOnlyTransaction;
import io.axway.iron.Store;
import io.axway.iron.core.StoreManagerBuilder;
import io.axway.iron.core.store.SucceedingStoreTest;

import static org.assertj.core.api.Assertions.assertThat;

public class ShouldAutoGenerateIdTest implements SucceedingStoreTest {
    private static final String VAL1 = "123";
    private static final String VAL2 = "456";

    @Override
    public void configure(StoreManagerBuilder builder) throws Exception {
        builder.withEntityClass(SimpleEntityWithId.class).withCommandClass(CommandCreateSimpleEntityWithId.class);
    }

    @Override
    public void execute(Store store) throws Exception {
        Long key1 = store.createCommand(CommandCreateSimpleEntityWithId.class).set(CommandCreateSimpleEntityWithId::value).to(VAL1).submit().get();
        Long key2 = store.createCommand(CommandCreateSimpleEntityWithId.class).set(CommandCreateSimpleEntityWithId::value).to(VAL2).submit().get();
        assertThat(key1).isEqualTo(0);
        assertThat(key2).isEqualTo(1);
    }

    @Override
    public void verify(ReadOnlyTransaction tx) {
        SimpleEntityWithId i1 = tx.select(SimpleEntityWithId.class).where(SimpleEntityWithId::id).equalsTo(0L);
        SimpleEntityWithId i2 = tx.select(SimpleEntityWithId.class).where(SimpleEntityWithId::id).equalsTo(1L);
        assertThat(i1.id()).isEqualTo(0);
        assertThat(i2.id()).isEqualTo(1);
        assertThat(i1.value()).isEqualTo(VAL1);
        assertThat(i2.value()).isEqualTo(VAL2);
    }
}
