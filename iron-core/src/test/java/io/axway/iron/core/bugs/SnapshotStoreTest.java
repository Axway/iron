package io.axway.iron.core.bugs;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import io.axway.iron.Store;
import io.axway.iron.StoreManager;
import io.axway.iron.core.StoreManagerFactoryBuilder;
import io.axway.iron.core.spi.file.FileStoreFactory;
import io.axway.iron.spi.jackson.JacksonSerializer;

import static org.assertj.core.api.Assertions.assertThat;

public class SnapshotStoreTest {
    private static final String MY_STORE = "my-store";
    private Path m_tempDir;

    @BeforeMethod
    public void setUp() throws Exception {
        m_tempDir = Paths.get("iron-" + getClass().getSimpleName() + "-" + UUID.randomUUID());
    }

    @AfterMethod
    public void tearDown() throws Exception {
        if (Files.isDirectory(m_tempDir)) {
            Files.walk(m_tempDir) //
                    .sorted(Comparator.reverseOrder()) //
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
    }

    @Test
    public void shouldAllowSnapshotCreationOnNewEmptyStore() throws Exception {
        // Given: a newly created store
        // When: creating a snapshot while store is still empty
        // Then: creating a snapshot should not fail

        try (StoreManager storeManager = createOpenStoreManager(MY_STORE)) {
            storeManager.snapshot();
        }
    }

    @Test
    public void shouldInitializeInitialTransactionIdAfterOpeningSnapshot() throws Exception {
        // Given: a store and a snapshot which transaction ID is TxID (TxID >= 4)
        // When: opening the store from that snapshot and creating a new snapshot without changing content of store
        // Then: new snapshot should be created with transaction ID >= TxID
        BigInteger transactionCount = BigInteger.TEN;

        try (StoreManager storeManager = createOpenStoreManager(MY_STORE)) {
            Store store = storeManager.getStore();
            for (int i = 0; i < transactionCount.longValueExact(); i++) {
                store.createCommand(SnapshotStoreCommand.class).set(SnapshotStoreCommand::value).to("value-" + i).submit().get();
            }

            // Create snapshot and close
            assertThat(storeManager.snapshot()).isEqualTo(transactionCount);
        }

        try (StoreManager storeManager = createOpenStoreManager(MY_STORE)) {

            // Should return the proper transaction ID
            assertThat(storeManager.lastSnapshotTransactionId()).isEqualTo(transactionCount);

            // Should not create another snapshot, since current snapshot is already the latest
            assertThat(storeManager.snapshot()).isNull();
        }
    }

    @Test
    public void shouldBeAbleToCallTwoConsecutiveTakeSnapshot() throws Exception {
        // Given: a empty store having one transaction with a snapshot
        // When: creating another snapshot
        // Then: no snapshot should be created since no modification since the last snapshot call

        try (StoreManager storeManager = createOpenStoreManager(MY_STORE)) {
            Store store = storeManager.getStore();
            store.createCommand(SnapshotStoreCommand.class).set(SnapshotStoreCommand::value).to("value").submit().get();

            // Should create snapshot 1
            assertThat(storeManager.snapshot()).isEqualTo(BigInteger.ONE);

            // Should not create another snapshot, since snapshot 1 is already the latest
            assertThat(storeManager.snapshot()).isNull();
        }
    }

    private StoreManager createOpenStoreManager(String storeName) throws Exception {
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        FileStoreFactory fileStoreFactory = new FileStoreFactory(m_tempDir);
        StoreManagerFactoryBuilder builder = StoreManagerFactoryBuilder.newStoreManagerBuilderFactory() //
                .withSnapshotSerializer(jacksonSerializer) //
                .withTransactionSerializer(jacksonSerializer) //
                .withSnapshotStoreFactory(fileStoreFactory) //
                .withTransactionStoreFactory(fileStoreFactory);
        builder.withEntityClass(SnapshotStoreEntityWithId.class).withCommandClass(SnapshotStoreCommand.class);
        return builder.build().openStore(storeName);
    }
}
