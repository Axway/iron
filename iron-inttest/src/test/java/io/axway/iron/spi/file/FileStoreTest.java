package io.axway.iron.spi.file;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import org.testng.annotations.Test;
import io.axway.iron.Store;
import io.axway.iron.StoreManager;
import io.axway.iron.core.StoreManagerBuilder;
import io.axway.iron.sample.command.ChangeCompanyAddress;
import io.axway.iron.sample.command.CreateCompany;
import io.axway.iron.sample.command.CreatePerson;
import io.axway.iron.sample.command.DeleteCompany;
import io.axway.iron.sample.command.PersonJoinCompany;
import io.axway.iron.sample.command.PersonLeaveCompany;
import io.axway.iron.sample.command.PersonRaiseSalary;
import io.axway.iron.sample.model.Company;
import io.axway.iron.sample.model.Person;
import io.axway.iron.spi.SpiTestHelper;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

import static io.axway.iron.spi.file.FileTestHelper.*;
import static io.axway.iron.spi.jackson.JacksonTestHelper.*;
import static java.nio.file.Paths.get;
import static java.util.Arrays.*;
import static java.util.Comparator.*;
import static java.util.stream.Collectors.*;
import static org.assertj.core.api.Assertions.assertThat;

public class FileStoreTest {

    @Test
    public void shouldCreateCompanySequenceBeRight() throws Exception {
        String randomStoreName = "iron-store-" + UUID.randomUUID();
        Path filePath = get("tmp-iron-test", "iron-spi-file-inttest");

        Supplier<TransactionStore> transactionStoreFactory = () -> buildFileTransactionStore(filePath, "shouldCreateCompanySequenceBeRight");
        Supplier<SnapshotStore> snapshotStoreFactory = () -> buildFileSnapshotStore(filePath, "shouldCreateCompanySequenceBeRight");

        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();

        SpiTestHelper.checkThatCreateCompanySequenceIsRight(transactionStoreFactory, transactionSerializer, snapshotStoreFactory, snapshotSerializer,
                                                            randomStoreName);
    }

    @Test
    public void shouldBeAbleToDeleteFilesAfterStoreManagerClose() throws IOException, InterruptedException {
        Path filePath = get("tmp-iron-test", "iron-spi-file-inttest");

        String directory = "iron-directory";
        Supplier<TransactionStore> transactionStoreFactory = () -> buildFileTransactionStore(filePath, directory);
        Supplier<SnapshotStore> snapshotStoreFactory = () -> buildFileSnapshotStore(filePath, directory);

        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();

        StoreManagerBuilder factoryBuilder = StoreManagerBuilder.newStoreManagerBuilder() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStore(transactionStoreFactory.get()) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStore(snapshotStoreFactory.get()) //
                .withEntityClass(Company.class) //
                .withEntityClass(Person.class) //
                .withCommandClass(ChangeCompanyAddress.class) //
                .withCommandClass(CreateCompany.class) //
                .withCommandClass(CreatePerson.class) //
                .withCommandClass(DeleteCompany.class) //
                .withCommandClass(PersonJoinCompany.class) //
                .withCommandClass(PersonLeaveCompany.class) //
                .withCommandClass(PersonRaiseSalary.class);

        try (StoreManager storeManager = factoryBuilder.build()) {
            storeManager.getStore("test");
            storeManager.snapshot();
        }

        assertThat(Files.walk(get("tmp-iron-test")).
                sorted(reverseOrder()).map(Path::toFile).
                map(file -> new Object[]{file, file.delete()})).
                allMatch(fileStatus -> (boolean) fileStatus[1]);
    }

    @Test
    public void shouldSnapshotContainAllInstances() throws IOException, ExecutionException, InterruptedException {
        // Given an iron store
        Path filePath = get("tmp-iron-test", "iron-spi-file-inttest");

        String directory = "iron-directory-" + UUID.randomUUID();
        Supplier<TransactionStore> transactionStoreFactory = () -> buildFileTransactionStore(filePath, directory);
        Supplier<SnapshotStore> snapshotStoreFactory = () -> buildFileSnapshotStore(filePath, directory);

        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();

        StoreManagerBuilder factoryBuilder = StoreManagerBuilder.newStoreManagerBuilder() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStore(transactionStoreFactory.get()) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStore(snapshotStoreFactory.get()) //
                .withEntityClass(Company.class) //
                .withEntityClass(Person.class) //
                .withCommandClass(CreatePerson.class) //
                ;

        try (StoreManager storeManager = factoryBuilder.build()) {
            Store store1 = storeManager.getStore("store1");
            Store store2 = storeManager.getStore("store2");
            // When I snapshot before any transaction
            storeManager.snapshot();
            // Then I find no snapshot
            assertThat(Files.walk(filePath.resolve(directory).resolve("snapshot")).collect(toSet())).
                    containsExactlyInAnyOrder(filePath.resolve(directory).resolve("snapshot"));
            //
            // When I snapshot after a transaction
            Store.TransactionBuilder transaction1 = store1.begin();
            transaction1.addCommand(CreatePerson.class).
                    set(CreatePerson::id).to("myPersonId1_1").
                    set(CreatePerson::name).to("myPersonName1_1").
                    submit();
            transaction1.submit().get();
            Store.TransactionBuilder transaction2 = store2.begin();
            transaction2.addCommand(CreatePerson.class).
                    set(CreatePerson::id).to("myPersonId2_1").
                    set(CreatePerson::name).to("myPersonName2_1").
                    submit();
            transaction2.submit().get();
            storeManager.snapshot();

            transaction1 = store1.begin();
            transaction1.addCommand(CreatePerson.class).
                    set(CreatePerson::id).to("myPersonId1_2").
                    set(CreatePerson::name).to("myPersonName1_2").
                    submit();
            transaction1.submit().get();
            storeManager.snapshot();
            // Then I find a snapshot
            assertThat(Files.walk(filePath.resolve(directory).resolve("snapshot")).collect(toSet())).
                    containsExactlyInAnyOrder(filePath.resolve(directory).resolve("snapshot"),
                                              filePath.resolve(directory).resolve("snapshot").resolve("00000000000000000000"),
                                              filePath.resolve(directory).resolve("snapshot").resolve("00000000000000000000").resolve("store1.snapshot"));
        }
    }
}


