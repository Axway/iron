package io.axway.iron.core.bugs;

import java.util.*;
import io.axway.iron.StoreManager;
import io.axway.iron.StoreManagerFactory;
import io.axway.iron.core.StoreManagerFactoryBuilder;
import io.axway.iron.core.model.simple.SimpleCommand;
import io.axway.iron.core.model.simple.SimpleEntity;
import io.axway.iron.core.spi.testing.TransientStoreFactory;
import io.axway.iron.spi.jackson.JacksonSerializer;

final class IronTestHelper {

    static StoreManager createTransientStore() {
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        TransientStoreFactory transientStoreFactory = new TransientStoreFactory();
        StoreManagerFactory storeManagerFactory = StoreManagerFactoryBuilder.newStoreManagerBuilderFactory() //
                .withSnapshotSerializer(jacksonSerializer).withTransactionSerializer(jacksonSerializer) //
                .withSnapshotStoreFactory(transientStoreFactory).withTransactionStoreFactory(transientStoreFactory) //
                .withCommandClass(SimpleCommand.class) //
                .withCommandClass(CommandWithLongParameterCommand.class) //
                .withCommandClass(CommandWithLongCollectionParameterCommand.class) //
                .withEntityClass(SimpleEntity.class) //
                .build();

        return storeManagerFactory.openStore("iron-test-bugs-" + UUID.randomUUID().toString());
    }

    private IronTestHelper() {
    }
}
