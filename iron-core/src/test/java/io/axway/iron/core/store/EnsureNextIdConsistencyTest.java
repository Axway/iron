package io.axway.iron.core.store;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.reactivestreams.Publisher;
import org.testng.annotations.Test;
import io.axway.iron.Store;
import io.axway.iron.StoreManager;
import io.axway.iron.core.StoreManagerBuilder;
import io.axway.iron.core.spi.testing.TransientTransactionStoreBuilder;
import io.axway.iron.core.store.id.CommandCreateSimpleEntityWithId;
import io.axway.iron.core.store.id.SimpleEntityWithId;
import io.axway.iron.error.UnrecoverableStoreException;
import io.axway.iron.spi.jackson.JacksonSnapshotSerializerBuilder;
import io.axway.iron.spi.jackson.JacksonTransactionSerializerBuilder;
import io.axway.iron.spi.storage.SnapshotStore;
import io.reactivex.Flowable;

import static org.assertj.core.api.Assertions.*;

import static java.nio.charset.StandardCharsets.UTF_8;

public class EnsureNextIdConsistencyTest {

    @Test
    public void shouldNotOpenStoreWithInconsistentNextId() throws Exception {
        AtomicReference<BigInteger> snapshotId = new AtomicReference<>();
        AtomicReference<byte[]> snapshotData = new AtomicReference<>();
        String storeName = "test";

        StoreManagerBuilder builder = StoreManagerBuilder.newStoreManagerBuilder();
        builder.withCommandClass(CommandCreateSimpleEntityWithId.class);
        builder.withEntityClass(SimpleEntityWithId.class);
        builder.withTransactionSerializer(new JacksonTransactionSerializerBuilder().get());
        builder.withSnapshotSerializer(new JacksonSnapshotSerializerBuilder().get());
        builder.withTransactionStore(new TransientTransactionStoreBuilder().get());
        builder.withSnapshotStore(new SnapshotStore() {
            @Override
            public OutputStream createSnapshotWriter(String storeName, BigInteger transactionId) {
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
            public Publisher<StoreSnapshotReader> createSnapshotReader(BigInteger transactionId) {
                return Flowable.just(new StoreSnapshotReader() {
                    @Override
                    public String storeName() {
                        return storeName;
                    }

                    @Override
                    public InputStream inputStream() {
                        return new ByteArrayInputStream(snapshotData.get());
                    }
                });
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

        try (StoreManager storeManager = builder.build()) {
            Store store = storeManager.getStore(storeName);
            store.createCommand(CommandCreateSimpleEntityWithId.class).set(CommandCreateSimpleEntityWithId::value).to("test1").submit().get();
            store.createCommand(CommandCreateSimpleEntityWithId.class).set(CommandCreateSimpleEntityWithId::value).to("test2").submit().get();
            storeManager.snapshot();
        }

        // corrupt snapshot nextId
        String snapshotStr = new String(snapshotData.get(), UTF_8);
        snapshotStr = snapshotStr.replace("\"nextId\":2", "\"nextId\":0");
        snapshotData.set(snapshotStr.getBytes(UTF_8));

        // this must fail
        try {
            builder.build();
            fail("Expected error due to snapshot corruption");
        } catch (Exception e) {
            assertThat(e)
                    .hasMessage("Error occurred when recovering from latest snapshot")
                    .hasCauseInstanceOf(UnrecoverableStoreException.class);
            assertThat(e.getCause()).hasMessage("Instance id is greater than or equals to nextId {\"args\": {\"instanceId\": 0, \"nextId\": 0}}");
        }
    }
}
