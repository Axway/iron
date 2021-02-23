package io.axway.iron.core.bugs;

import java.util.*;
import org.testng.annotations.Test;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.Store;
import io.axway.iron.StoreManager;
import io.axway.iron.description.Entity;
import io.axway.iron.description.Unique;

import static org.assertj.core.api.Assertions.assertThatCode;

public class UniqueConstraintTest {
    @Test
    public void ensureUniqueConstraintCacheProblemIsSolved() throws Exception {
        try (StoreManager storeManager = IronTestHelper.createTransientStore(List.of(SimpleEntity.class), List.of(CreateSimpleEntity.class))) {
            Store store = IronTestHelper.getRandomTransientStore(storeManager);
            store.createCommand(CreateSimpleEntity.class).
                    set(CreateSimpleEntity::name).to("Bruce Banner").
                    set(CreateSimpleEntity::login).to("Hulk").
                    submit().get();

            assertThatCode(() -> store.createCommand(CreateSimpleEntity.class).
                    set(CreateSimpleEntity::name).to("Bruce Banner").
                    set(CreateSimpleEntity::login).to("IronMan").
                    submit().get()).
                    withFailMessage("Using the same name 'Bruce Banner' should trigger an exception").
                    hasMessageContaining("Unique constraint violation");

            assertThatCode(() -> store.createCommand(CreateSimpleEntity.class).
                    set(CreateSimpleEntity::name).to("Tony Stark").
                    set(CreateSimpleEntity::login).to("Hulk").
                    submit().get()).
                    withFailMessage("Using the same login 'Hulk' should trigger an exception").
                    hasMessageContaining("Unique constraint violation");

            assertThatCode(() -> store.createCommand(CreateSimpleEntity.class).
                    set(CreateSimpleEntity::name).to("Tony Stark").
                    set(CreateSimpleEntity::login).to("IronMan").
                    submit().get()).
                    withFailMessage("Tony should be able to use its own login because nothing is conflicting with Bruce's one").
                    doesNotThrowAnyException();
        }
    }

    @Entity
    public interface SimpleEntity {
        @Unique
        String name();

        @Unique
        String secret();
    }

    public interface CreateSimpleEntity extends Command<SimpleEntity> {
        String name();

        String login();

        @Override
        default SimpleEntity execute(ReadWriteTransaction tx) {
            return tx.insert(SimpleEntity.class).set(SimpleEntity::name).to(name()).set(SimpleEntity::secret).to(login()).done();
        }
    }
}
