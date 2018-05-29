package io.axway.iron.core;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.axway.alf.exception.IllegalArgumentFormattedException;
import io.axway.alf.exception.IllegalStateFormattedException;
import io.axway.iron.StoreManager;
import io.axway.iron.core.model.simple.SimpleCommand;
import io.axway.iron.core.model.simple.SimpleEntity;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

import static io.axway.iron.core.bugs.IronTestHelper.*;
import static org.assertj.core.api.Assertions.assertThat;

public class StoreManagerTest {

    @DataProvider(name = "duplicates")
    public Object[][] providesDuplicates() {
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotStore snapshotStore = buildTransientSnapshotStoreFactory();
        TransactionStore transactionStore = buildTransientTransactionStoreFactory();

        StoreManagerBuilder b = StoreManagerBuilder.newStoreManagerBuilder();

        return new Runnable[][]{{() -> b.withSnapshotSerializer(snapshotSerializer)}, //
                {() -> b.withTransactionSerializer(transactionSerializer)}, //
                {() -> b.withSnapshotStore(snapshotStore)}, //
                {() -> b.withTransactionStore(transactionStore)}, //
                {() -> b.withCommandClass(SimpleCommand.class)}, //
                {() -> b.withEntityClass(SimpleEntity.class)}, //
        };
    }

    @Test(dataProvider = "duplicates", expectedExceptions = IllegalStateFormattedException.class)
    public void shouldBuilderNotAcceptDuplicateCalls(Runnable r) {
        r.run();
        r.run();
    }

    @DataProvider(name = "configuredElements")
    public Object[][] providesConfiguredElements() {
        return new Object[][]{{false, true, true, true}, //
                {true, false, true, true}, //
                {true, true, false, true}, //
                {true, true, true, false}, //
        };
    }

    @Test(dataProvider = "configuredElements", expectedExceptions = IllegalStateFormattedException.class)
    public void shouldBuilderNotAcceptIncompleteConfiguration(boolean b1, boolean b2, boolean b3, boolean b4) {
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotStore snapshotStore = buildTransientSnapshotStoreFactory();
        TransactionStore transactionStore = buildTransientTransactionStoreFactory();

        StoreManagerBuilder builder = StoreManagerBuilder.newStoreManagerBuilder();
        if (b1) {
            builder.withSnapshotSerializer(snapshotSerializer);
        }

        if (b2) {
            builder.withTransactionSerializer(transactionSerializer);
        }

        if (b3) {
            builder.withSnapshotStore(snapshotStore);
        }

        if (b4) {
            builder.withTransactionStore(transactionStore);
        }

        builder.withCommandClass(SimpleCommand.class) //
                .withEntityClass(SimpleEntity.class) //
                .build();
    }

    private StoreManager createStoreManagerFactory() {
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotStore snapshotStore = buildTransientSnapshotStoreFactory();
        TransactionStore transactionStore = buildTransientTransactionStoreFactory();

        return StoreManagerBuilder.newStoreManagerBuilder() //
                .withSnapshotSerializer(snapshotSerializer) //
                .withTransactionSerializer(transactionSerializer) //
                .withSnapshotStore(snapshotStore) //
                .withTransactionStore(transactionStore) //
                .withCommandClass(SimpleCommand.class) //
                .withEntityClass(SimpleEntity.class) //
                .build();
    }

    @DataProvider(name = "invalidStoreNames")
    public Object[][] providesInvalidStoreNames() {
        return new Object[][]{{ //
                "."}, //
                {"/"}, //
                {"\\"}, //
                {":"}, //
                {" "}, //
        };
    }

    @Test(dataProvider = "invalidStoreNames")
    public void shouldNotOpenStoreWithInvalidName(String storeName) {
        try (StoreManager ignored = createStoreManagerFactory()) {
            ignored.getStore(storeName);
        } catch(UncheckedExecutionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalArgumentFormattedException.class);
            assertThat(e.getCause()).hasMessageStartingWith("Invalid store name");
        }
    }

    @Test
    public void shouldOpenStoreWithValidName() {
        try (StoreManager ignored = createStoreManagerFactory()) {
            ignored.getStore("a-zA-Z0-9_");
        }
    }
}
