package io.axway.iron.core;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import io.axway.iron.StoreManager;
import io.axway.iron.StoreManagerFactory;
import io.axway.iron.core.model.simple.SimpleCommand;
import io.axway.iron.core.model.simple.SimpleEntity;
import io.axway.iron.core.spi.testing.TransientStoreFactory;
import io.axway.iron.spi.jackson.JacksonSerializer;

public class StoreManagerFactoryTest {

    @DataProvider(name = "duplicates")
    public Object[][] providesDuplicates() {
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        TransientStoreFactory transientStoreFactory = new TransientStoreFactory();
        StoreManagerFactoryBuilder b = StoreManagerFactoryBuilder.newStoreManagerBuilderFactory();

        return new Runnable[][]{{() -> b.withSnapshotSerializer(jacksonSerializer)}, //
                {() -> b.withTransactionSerializer(jacksonSerializer)}, //
                {() -> b.withSnapshotStoreFactory(transientStoreFactory)}, //
                {() -> b.withTransactionStoreFactory(transientStoreFactory)}, //
                {() -> b.withCommandClass(SimpleCommand.class)}, //
                {() -> b.withEntityClass(SimpleEntity.class)}, //
        };
    }

    @Test(dataProvider = "duplicates", expectedExceptions = IllegalStateException.class)
    public void shouldBuilderNotAcceptDuplicateCalls(Runnable r) throws Exception {
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
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        TransientStoreFactory transientStoreFactory = new TransientStoreFactory();
        StoreManagerFactoryBuilder builder = StoreManagerFactoryBuilder.newStoreManagerBuilderFactory();
        if (b1) {
            builder.withSnapshotSerializer(jacksonSerializer);
        }

        if (b2) {
            builder.withTransactionSerializer(jacksonSerializer);
        }

        if (b3) {
            builder.withSnapshotStoreFactory(transientStoreFactory);
        }

        if (b4) {
            builder.withTransactionStoreFactory(transientStoreFactory);
        }

        builder.withCommandClass(SimpleCommand.class) //
                .withEntityClass(SimpleEntity.class) //
                .build();
    }

    private StoreManagerFactory createStoreManagerFactory() {
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        TransientStoreFactory transientStoreFactory = new TransientStoreFactory();
        return StoreManagerFactoryBuilder.newStoreManagerBuilderFactory() //
                .withSnapshotSerializer(jacksonSerializer) //
                .withTransactionSerializer(jacksonSerializer) //
                .withSnapshotStoreFactory(transientStoreFactory) //
                .withTransactionStoreFactory(transientStoreFactory) //
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
