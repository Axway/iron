package io.axway.iron.core.bugs;

import java.nio.file.Path;
import java.util.*;
import io.axway.iron.Store;
import io.axway.iron.StoreManager;
import io.axway.iron.core.StoreManagerBuilder;
import io.axway.iron.core.model.simple.SimpleCommand;
import io.axway.iron.core.model.simple.SimpleEntity;
import io.axway.iron.core.spi.file.FileSnapshotStoreBuilder;
import io.axway.iron.core.spi.file.FileTransactionStoreBuilder;
import io.axway.iron.core.spi.testing.TransientSnapshotStoreBuilder;
import io.axway.iron.core.spi.testing.TransientTransactionStoreBuilder;
import io.axway.iron.spi.jackson.JacksonSnapshotSerializerBuilder;
import io.axway.iron.spi.jackson.JacksonTransactionSerializerBuilder;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

final public class IronTestHelper {

    static StoreManager createTransientStore() {
       return StoreManagerBuilder.newStoreManagerBuilder() //
                .withSnapshotSerializer(buildJacksonSnapshotSerializer())       //
                .withTransactionSerializer(buildJacksonTransactionSerializer()) //
                .withSnapshotStore(buildTransientSnapshotStoreFactory())       //
                .withTransactionStore(buildTransientTransactionStoreFactory()) //
                .withCommandClass(SimpleCommand.class) //
                .withCommandClass(CommandWithLongParameterCommand.class) //
                .withCommandClass(CommandWithLongCollectionParameterCommand.class) //
                .withEntityClass(SimpleEntity.class) //
                .build();
    }

    static Store getRandomTransientStore(StoreManager storeManager){
        return storeManager.getStore("iron-test-bugs-" + UUID.randomUUID().toString());
    }

    public static SnapshotSerializer buildJacksonSnapshotSerializer() {
        return new JacksonSnapshotSerializerBuilder().get();
    }

    public static TransactionSerializer buildJacksonTransactionSerializer() {
        return new JacksonTransactionSerializerBuilder().get();
    }

    public static SnapshotStore buildTransientSnapshotStoreFactory() {
        return new TransientSnapshotStoreBuilder().get();
    }

    public static TransactionStore buildTransientTransactionStoreFactory() {
        return new TransientTransactionStoreBuilder().get();
    }

    public static SnapshotStore buildFileSnapshotStoreFactory(Path filePath, String name) {
        return new FileSnapshotStoreBuilder(name).setDir(filePath).get();
    }

    public static TransactionStore buildFileTransactionStoreFactory(Path filePath, String name) {
        return new FileTransactionStoreBuilder(name).setDir(filePath).get();
    }

    private IronTestHelper() {
    }
}
