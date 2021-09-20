package io.axway.iron.spi.file;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import org.testng.annotations.Test;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.Store;
import io.axway.iron.StoreManager;
import io.axway.iron.core.StoreManagerBuilder;
import io.axway.iron.description.Entity;
import io.axway.iron.description.Id;
import io.axway.iron.description.Unique;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

import static io.axway.iron.spi.jackson.JacksonTestHelper.*;
import static org.assertj.core.api.Assertions.assertThat;

public class ManyTransactionsIntTest {

    /**
     * This test is to ensure that an issue with a transaction garbaged
     */
    @Test
    public void shouldNotStuckInCaseOfManyTransactions() throws InterruptedException, ExecutionException, TimeoutException {
        String randomStoreName = "iron-store-" + UUID.randomUUID();
        Path filePath = Paths.get("iron", "iron-spi-file-inttest");

        TransactionStore transactionStore = FileTestHelper.buildFileTransactionStore(filePath, randomStoreName);
        SnapshotStore snapshotStore = FileTestHelper.buildFileSnapshotStore(filePath, randomStoreName);

        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();

        try (StoreManager storeManager = StoreManagerBuilder.newStoreManagerBuilder() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStore(transactionStore) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStore(snapshotStore) //
                .withEntityClass(SimpleEntity.class) //
                .withCommandClass(CreateSimpleEntity.class) //
                .build()) {

            for (long j = 0; j < 100_000; j++) {
                Store store = storeManager.getStore(randomStoreName);
                Long id = store.createCommand(CreateSimpleEntity.class).set(CreateSimpleEntity::name).to(UUID.randomUUID().toString()).submit()
                        .get(10, TimeUnit.SECONDS);
                assertThat(j).as("The command index " + j + " should increase like the instance id " + id).isEqualTo(id);
                if (j % 10_000 == 0) {
                    System.out.println("j=" + j + "; id=" + id);
                }
            }
            storeManager.snapshot();
        }
    }

    public interface CreateSimpleEntity extends Command<Long> {

        String name();

        @Override
        default Long execute(ReadWriteTransaction tx) {

            SimpleEntity simpleEntity = tx.insert(SimpleEntity.class) //
                    .set(SimpleEntity::name).to(name()) //
                    .done();

            return simpleEntity.id();
        }
    }

    @Entity
    public interface SimpleEntity {
        @Id
        long id();

        @Unique
        String name();
    }
}
