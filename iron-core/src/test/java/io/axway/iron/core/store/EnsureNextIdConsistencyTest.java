package io.axway.iron.core.store;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.testng.annotations.Test;
import io.axway.iron.Store;
import io.axway.iron.StoreManager;
import io.axway.iron.StoreManagerFactory;
import io.axway.iron.core.StoreManagerFactoryBuilder;
import io.axway.iron.core.spi.testing.TransientTransactionStoreFactoryBuilder;
import io.axway.iron.core.store.id.CommandCreateSimpleEntityWithId;
import io.axway.iron.core.store.id.SimpleEntityWithId;
import io.axway.iron.error.UnrecoverableStoreException;
import io.axway.iron.spi.jackson.JacksonSnapshotSerializerBuilder;
import io.axway.iron.spi.jackson.JacksonTransactionSerializerBuilder;
import io.axway.iron.spi.storage.SnapshotStore;

import static java.nio.charset.StandardCharsets.UTF_8;

public class EnsureNextIdConsistencyTest {

    @Test(expectedExceptions = UnrecoverableStoreException.class, expectedExceptionsMessageRegExp = "Instance id is greater than or equals to nextId.*")
    public void shouldNotOpenStoreWithInconsistentNextId() throws Exception {
        AtomicReference<BigInteger> snapshotId = new AtomicReference<>();
        AtomicReference<byte[]> snapshotData = new AtomicReference<>();

        StoreManagerFactoryBuilder builder = StoreManagerFactoryBuilder.newStoreManagerBuilderFactory();
        builder.withCommandClass(CommandCreateSimpleEntityWithId.class);
        builder.withEntityClass(SimpleEntityWithId.class);
        builder.withTransactionSerializer(new JacksonTransactionSerializerBuilder().get());
        builder.withSnapshotSerializer(new JacksonSnapshotSerializerBuilder().get());
        builder.withTransactionStoreFactory(new TransientTransactionStoreFactoryBuilder().get());
        builder.withSnapshotStoreFactory(storeName -> new SnapshotStore() {
            @Override
            public OutputStream createSnapshotWriter(BigInteger transactionId) throws IOException {
                return new ByteArrayOutputStream() {
                    @Override
                    public void close() throws IOException {
                        super.close();
                        snapshotData.set(toByteArray());
                        snapshotId.set(transactionId);
                    }
                };
            }

            @Override
            public InputStream createSnapshotReader(BigInteger transactionId) throws IOException {
                return new ByteArrayInputStream(snapshotData.get());
            }

            @Override
            public List<BigInteger> listSnapshots() {
                if (snapshotId.get() != null) {
                    return Collections.singletonList(snapshotId.get());
                } else {
                    return Collections.emptyList();
                }
            }

            @Override
            public void deleteSnapshot(BigInteger transactionId) {
                // nothing to do
            }
        });

        StoreManagerFactory storeManagerFactory = builder.build();

        try (StoreManager testStoreManager = storeManagerFactory.openStore("test")) {
            Store store = testStoreManager.getStore();
            store.createCommand(CommandCreateSimpleEntityWithId.class).set(CommandCreateSimpleEntityWithId::value).to("test1").submit().get();
            store.createCommand(CommandCreateSimpleEntityWithId.class).set(CommandCreateSimpleEntityWithId::value).to("test2").submit().get();
            testStoreManager.snapshot();
        }

        // corrupt snapshot nextId
        String snapshotStr = new String(snapshotData.get(), UTF_8);
        snapshotStr = snapshotStr.replace("\"nextId\":2", "\"nextId\":0");
        snapshotData.set(snapshotStr.getBytes(UTF_8));

        // this must fail
        storeManagerFactory.openStore("test");
    }
}
