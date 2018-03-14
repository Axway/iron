package io.axway.iron.core;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import io.axway.iron.StoreManager;
import io.axway.iron.StoreManagerFactory;
import io.axway.iron.core.model.simple.SimpleCommand;
import io.axway.iron.core.model.simple.SimpleEntity;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static io.axway.iron.core.bugs.IronTestHelper.*;

public class StoreManagerFactoryTest {

    @DataProvider(name = "duplicates")
    public Object[][] providesDuplicates() {
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotStoreFactory snapshotStoreFactory = buildTransientSnapshotStoreFactory();
        TransactionStoreFactory transactionStoreFactory = buildTransientTransactionStoreFactory();

        StoreManagerFactoryBuilder b = StoreManagerFactoryBuilder.newStoreManagerBuilderFactory();

        return new Runnable[][]{{() -> b.withSnapshotSerializer(snapshotSerializer)}, //
                {() -> b.withTransactionSerializer(transactionSerializer)}, //
                {() -> b.withSnapshotStoreFactory(snapshotStoreFactory)}, //
                {() -> b.withTransactionStoreFactory(transactionStoreFactory)}, //
                {() -> b.withCommandClass(SimpleCommand.class)}, //
                {() -> b.withEntityClass(SimpleEntity.class)}, //
        };
    }

    @Test(dataProvider = "duplicates", expectedExceptions = IllegalStateException.class)
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

    @Test(dataProvider = "configuredElements", expectedExceptions = IllegalStateException.class)
    public void shouldBuilderNotAcceptIncompleteConfiguration(boolean b1, boolean b2, boolean b3, boolean b4) {
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotStoreFactory snapshotStoreFactory = buildTransientSnapshotStoreFactory();
        TransactionStoreFactory transactionStoreFactory = buildTransientTransactionStoreFactory();

        StoreManagerFactoryBuilder builder = StoreManagerFactoryBuilder.newStoreManagerBuilderFactory();
        if (b1) {
            builder.withSnapshotSerializer(snapshotSerializer);
        }

        if (b2) {
            builder.withTransactionSerializer(transactionSerializer);
        }

        if (b3) {
            builder.withSnapshotStoreFactory(snapshotStoreFactory);
        }

        if (b4) {
            builder.withTransactionStoreFactory(transactionStoreFactory);
        }

        builder.withCommandClass(SimpleCommand.class) //
                .withEntityClass(SimpleEntity.class) //
                .build();
    }

    private StoreManagerFactory createStoreManagerFactory() {
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotStoreFactory snapshotStoreFactory = buildTransientSnapshotStoreFactory();
        TransactionStoreFactory transactionStoreFactory = buildTransientTransactionStoreFactory();

        return StoreManagerFactoryBuilder.newStoreManagerBuilderFactory() //
                .withSnapshotSerializer(snapshotSerializer) //
                .withTransactionSerializer(transactionSerializer) //
                .withSnapshotStoreFactory(snapshotStoreFactory) //
                .withTransactionStoreFactory(transactionStoreFactory) //
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

    @Test(dataProvider = "invalidStoreNames", expectedExceptions = IllegalArgumentException.class)
    public void shouldNotOpenStoreWithInvalidName(String storeName) {
        //noinspection EmptyTryBlock
        try (StoreManager ignored = createStoreManagerFactory().openStore(storeName)) {
        }
    }

    @Test
    public void shouldOpenStoreWithValidName() {
        //noinspection EmptyTryBlock
        try (StoreManager ignored = createStoreManagerFactory().openStore("a-zA-Z0-9_")) {
        }
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void shouldFailWhenOpeningStoreTwiceAtSameTime() {
        StoreManagerFactory factory = createStoreManagerFactory();

        //noinspection EmptyTryBlock
        try (StoreManager ignored = factory.openStore("test"); StoreManager ignored2 = factory.openStore("test")) {
        }
    }

    @Test
    public void shouldOpenSameStoreAfterClose() {
        StoreManagerFactory factory = createStoreManagerFactory();
        //noinspection EmptyTryBlock
        try (StoreManager ignored = factory.openStore("test")) {
        }

        //noinspection EmptyTryBlock
        try (StoreManager ignored = factory.openStore("test")) {
        }
    }
}
