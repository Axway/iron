package io.axway.iron.core.management;

import java.util.*;
import org.testng.annotations.Test;
import io.axway.iron.Store;
import io.axway.iron.StoreManager;
import io.axway.iron.core.bugs.IronTestHelper;
import io.axway.iron.core.model.simple.CreateSimpleEntity;
import io.axway.iron.core.model.simple.SimpleEntity;

import static org.assertj.core.api.Assertions.*;

public class ReadonlySwitchTest {

    @Test
    public void shouldStoreManagerHandleReadonlySwitch() throws Exception {
        try (StoreManager storeManager = IronTestHelper.createTransientStore()) {
            Store store = IronTestHelper.getRandomTransientStore(storeManager);

            assertThat(storeManager.isReadOnly()).isEqualTo(false);

            String simpleEntityId = "shouldBeCreated";
            store.createCommand(CreateSimpleEntity.class).set(CreateSimpleEntity::id).to(simpleEntityId).submit().get();

            storeManager.setReadonly(true);

            assertThatCode(() -> store.createCommand(CreateSimpleEntity.class).
                    set(CreateSimpleEntity::id).to("willBeRejected").
                    submit().get()).
                    withFailMessage("Store shouldn't accept command while in readonly").
                    hasMessageContaining("ReadWriteTransaction can't be executed, store is in readonly");

            assertThatCode(() -> {
                Collection<SimpleEntity> entities = store.query(readOnlyTransaction -> {
                    return readOnlyTransaction.select(SimpleEntity.class).all();
                });

                assertThat(entities.size()).isEqualTo(1);
                assertThat(entities.stream().findFirst().orElseThrow().id()).isEqualTo(simpleEntityId);
                assertThat(storeManager.isReadOnly()).isEqualTo(true);
            }).
                    withFailMessage("Store should continue to accept query even in Maintenance mode").
                    doesNotThrowAnyException();
        }
    }
}
