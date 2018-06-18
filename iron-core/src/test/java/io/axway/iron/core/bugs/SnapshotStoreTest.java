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
import io.axway.iron.core.StoreManagerBuilder;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

import static io.axway.iron.core.bugs.IronTestHelper.*;
import static java.math.BigInteger.*;
import static org.assertj.core.api.Assertions.assertThat;

public class SnapshotStoreTest {
    private static final String MY_STORE = "my-store";
    private Path m_tempDir;

    @BeforeMethod
    public void setUp() {
        m_tempDir = Paths.get("tmp-iron-test", "iron-" + getClass().getSimpleName() + "-" + UUID.randomUUID());
    }

    @AfterMethod
    public void tearDown() throws Exception {
        Files.walk(m_tempDir)                            //
                .sorted(Comparator.reverseOrder())       //
                .map(Path::toFile)                       //
                .forEach(File::delete);
    }

    @Test
    public void shouldAllowSnapshotCreationOnNewEmptyStore() {
        // Given: a newly created store
        // When: creating a snapshot while store is still empty
        // Then: creating a snapshot should not fail

        try (StoreManager storeManager = createOpenStoreManager("shouldAllowSnapshotCreationOnNewEmptyStore")) {
            storeManager.getStore(MY_STORE);
            storeManager.snapshot();
        }
    }

    @Test
    public void shouldInitializeInitialTransactionIdAfterOpeningSnapshot() throws Exception {
        // Given: a store and a snapshot which transaction ID is TxID (TxID >= 4)
        // When: opening the store from that snapshot and creating a new snapshot without changing content of store
        // Then: new snapshot should be created with transaction ID >= TxID
        BigInteger transactionCount = TEN;

        try (StoreManager storeManager = createOpenStoreManager("shouldInitializeInitialTransactionIdAfterOpeningSnapshot")) {
            Store store = storeManager.getStore(MY_STORE);
            for (int i = 0; i < transactionCount.longValueExact(); i++) {
                store.createCommand(SnapshotStoreCommand.class).set(SnapshotStoreCommand::value).to("value-" + i).submit().get();
            }

            // Create snapshot and close
            assertThat(storeManager.snapshot()).isEqualTo(transactionCount.subtract(ONE));
        }

        try (StoreManager storeManager = createOpenStoreManager("shouldInitializeInitialTransactionIdAfterOpeningSnapshot")) {

            // Should return the proper transaction ID
            assertThat(storeManager.lastSnapshotTransactionId()).isEqualTo(transactionCount.subtract(ONE));

            // Should not create another snapshot, since current snapshot is already the latest
            assertThat(storeManager.snapshot()).isNull();
        }
    }

    @Test
    public void shouldBeAbleToCallTwoConsecutiveTakeSnapshot() throws Exception {
        // Given: a empty store having one transaction with a snapshot
        // When: creating another snapshot
        // Then: no snapshot should be created since no modification since the last snapshot call

        try (StoreManager storeManager = createOpenStoreManager("shouldBeAbleToCallTwoConsecutiveTakeSnapshot")) {
            Store store = storeManager.getStore(MY_STORE);
            store.createCommand(SnapshotStoreCommand.class).set(SnapshotStoreCommand::value).to("value").submit().get();

            // Should create snapshot 1
            assertThat(storeManager.snapshot()).isEqualTo(ZERO);

            // Should not create another snapshot, since snapshot 1 is already the latest
            assertThat(storeManager.snapshot()).isNull();

            store.createCommand(SnapshotStoreCommand.class).set(SnapshotStoreCommand::value).to("value2").submit().get();

            assertThat(storeManager.snapshot()).isEqualTo(ONE);
        }
    }

    private StoreManager createOpenStoreManager(String factoryName) {
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotStore snapshotStore = buildFileSnapshotStoreFactory(m_tempDir, factoryName);
        TransactionStore transactionStore = buildFileTransactionStoreFactory(m_tempDir, factoryName);

        StoreManagerBuilder builder = StoreManagerBuilder.newStoreManagerBuilder() //
                .withSnapshotSerializer(snapshotSerializer) //
                .withTransactionSerializer(transactionSerializer) //
                .withSnapshotStore(snapshotStore) //
                .withTransactionStore(transactionStore);
        builder.withEntityClass(SnapshotStoreEntityWithId.class).withCommandClass(SnapshotStoreCommand.class);
        return builder.build();
    }
}
