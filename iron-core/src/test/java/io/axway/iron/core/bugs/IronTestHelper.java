package io.axway.iron.core.bugs;

import java.nio.file.Path;
import java.util.*;
import io.axway.iron.StoreManager;
import io.axway.iron.StoreManagerFactory;
import io.axway.iron.core.StoreManagerFactoryBuilder;
import io.axway.iron.core.model.simple.SimpleCommand;
import io.axway.iron.core.model.simple.SimpleEntity;
import io.axway.iron.core.spi.file.FileSnapshotStoreFactoryBuilder;
import io.axway.iron.core.spi.file.FileTransactionStoreFactoryBuilder;
import io.axway.iron.core.spi.testing.TransientSnapshotStoreFactoryBuilder;
import io.axway.iron.core.spi.testing.TransientTransactionStoreFactoryBuilder;
import io.axway.iron.spi.jackson.JacksonSnapshotSerializerBuilder;
import io.axway.iron.spi.jackson.JacksonTransactionSerializerBuilder;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStoreFactory;

final public class IronTestHelper {

    static StoreManager createTransientStore() {
        StoreManagerFactory storeManagerFactory = StoreManagerFactoryBuilder.newStoreManagerBuilderFactory() //
                .withSnapshotSerializer(buildJacksonSnapshotSerializer())       //
                .withTransactionSerializer(buildJacksonTransactionSerializer()) //
                .withSnapshotStoreFactory(buildTransientSnapshotStoreFactory())       //
                .withTransactionStoreFactory(buildTransientTransactionStoreFactory()) //
                .withCommandClass(SimpleCommand.class) //
                .withCommandClass(CommandWithLongParameterCommand.class) //
                .withCommandClass(CommandWithLongCollectionParameterCommand.class) //
                .withEntityClass(SimpleEntity.class) //
                .build();

        return storeManagerFactory.openStore("iron-test-bugs-" + UUID.randomUUID().toString());
    }

    public static SnapshotSerializer buildJacksonSnapshotSerializer() {
        return new JacksonSnapshotSerializerBuilder().get();
    }

    public static TransactionSerializer buildJacksonTransactionSerializer() {
        return new JacksonTransactionSerializerBuilder().get();
    }

    public static SnapshotStoreFactory buildTransientSnapshotStoreFactory() {
        return new TransientSnapshotStoreFactoryBuilder().get();
    }

    public static TransactionStoreFactory buildTransientTransactionStoreFactory() {
        return new TransientTransactionStoreFactoryBuilder().get();
    }

    public static SnapshotStoreFactory buildFileSnapshotStoreFactory(Path filePath) {
        return new FileSnapshotStoreFactoryBuilder().setDir(filePath).get();
    }

    public static TransactionStoreFactory buildFileTransactionStoreFactory(Path filePath) {
        return new FileTransactionStoreFactoryBuilder().setDir(filePath).get();
    }

    private IronTestHelper() {
    }
}
