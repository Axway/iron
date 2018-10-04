package io.axway.iron.spi.file;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import static java.util.Comparator.*;
import static java.util.stream.Collectors.*;
import static org.assertj.core.api.Assertions.assertThat;

public class FileStoreTest {

    @Test
    public void shouldCreateCompanySequenceBeRight() throws Exception {
        String randomStoreName = "iron-store-" + UUID.randomUUID();
        Path filePath = Paths.get("tmp-iron-test", "iron-spi-file-inttest");

        Supplier<TransactionStore> transactionStoreFactory = () -> buildFileTransactionStore(filePath, "shouldCreateCompanySequenceBeRight");
        Supplier<SnapshotStore> snapshotStoreFactory = () -> buildFileSnapshotStore(filePath, "shouldCreateCompanySequenceBeRight");

        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();

        SpiTestHelper.checkThatCreateCompanySequenceIsRight(transactionStoreFactory, transactionSerializer, snapshotStoreFactory, snapshotSerializer,
                                                            randomStoreName);
    }

    @Test
    public void shouldBeAbleToDeleteFilesAfterStoreManagerClose() throws IOException {
        Path filePath = Paths.get("tmp-iron-test", "iron-spi-file-inttest-" + UUID.randomUUID());

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

        assertThat(Files.walk(filePath).
                sorted(reverseOrder()).map(Path::toFile).
                map(file -> new Object[]{file, file.delete()})).
                allMatch(fileStatus -> (boolean) fileStatus[1]);
    }

    @Test
    public void shouldASnapshotCommandWaitATransactionToGenerateASnapshot() throws IOException, ExecutionException, InterruptedException {
        // Given an iron store
        Path filePath = Paths.get("tmp-iron-test", "iron-spi-file-inttest");

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
            storeManager.snapshot();

            // Then I find a snapshot
            assertThat(Files.walk(filePath.resolve(directory).resolve("snapshot")).collect(toSet())).
                    containsExactlyInAnyOrder(filePath.resolve(directory).resolve("snapshot"),
                                              filePath.resolve(directory).resolve("snapshot").resolve("00000000000000000000"),
                                              filePath.resolve(directory).resolve("snapshot").resolve("00000000000000000000").resolve("store1.snapshot"));
        }
    }

    @Test
    public void shouldSnapshotContainAllInstances() throws IOException, ExecutionException, InterruptedException {
        // Given an iron store
        Path filePath = Paths.get("tmp-iron-test", "iron-spi-file-inttest");

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
                    containsExactlyInAnyOrder(//
                                              filePath.resolve(directory).resolve("snapshot"),
                                              filePath.resolve(directory).resolve("snapshot").resolve("00000000000000000001"),
                                              filePath.resolve(directory).resolve("snapshot").resolve("00000000000000000001").resolve("store1.snapshot"),
                                              filePath.resolve(directory).resolve("snapshot").resolve("00000000000000000001").resolve("store2.snapshot"),
                                              filePath.resolve(directory).resolve("snapshot").resolve("00000000000000000002"),
                                              filePath.resolve(directory).resolve("snapshot").resolve("00000000000000000002").resolve("store1.snapshot"),
                                              filePath.resolve(directory).resolve("snapshot").resolve("00000000000000000002").resolve("store2.snapshot")
                                              //
                    );
        }
    }

    @Test
    public void shouldIronStartWithALackingTransaction() throws Exception {
        // Given an iron store
        Path filePath = Paths.get("tmp-iron-test", "iron-spi-file-inttest");

        String directory = "iron-directory-lacking-transaction-" + UUID.randomUUID();
        Supplier<TransactionStore> transactionStoreFactory = () -> buildFileTransactionStore(filePath, directory);
        Supplier<SnapshotStore> snapshotStoreFactory = () -> buildFileSnapshotStore(filePath, directory);

        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();

        // When
        // - I add a transaction and then do a snapshot
        StoreManagerBuilder factoryBuilder1 = StoreManagerBuilder.newStoreManagerBuilder() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStore(transactionStoreFactory.get()) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStore(snapshotStoreFactory.get()) //
                .withEntityClass(Company.class) //
                .withEntityClass(Person.class) //
                .withCommandClass(CreatePerson.class) //
                ;
        try (StoreManager storeManager = factoryBuilder1.build()) {
            Store store1 = storeManager.getStore("store1");
            Store.TransactionBuilder transaction1_1 = store1.begin();
            transaction1_1.addCommand(CreatePerson.class).
                    set(CreatePerson::id).to("myPersonId1_1").
                    set(CreatePerson::name).to("myPersonName1_1").
                    submit();
            transaction1_1.submit().get();

            Store store2 = storeManager.getStore("store2");
            Store.TransactionBuilder transaction2_1 = store2.begin();
            transaction2_1.addCommand(CreatePerson.class).
                    set(CreatePerson::id).to("myPersonId2_1").
                    set(CreatePerson::name).to("myPersonName2_1").
                    submit();
            transaction2_1.submit().get();
            storeManager.snapshot();
        }

        // - I add a transaction but I do not do a snapshot
        StoreManagerBuilder factoryBuilder2 = StoreManagerBuilder.newStoreManagerBuilder() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStore(transactionStoreFactory.get()) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStore(snapshotStoreFactory.get()) //
                .withEntityClass(Company.class) //
                .withEntityClass(Person.class) //
                .withCommandClass(CreatePerson.class) //
                ;
        try (StoreManager storeManager = factoryBuilder2.build()) {
            Store store1 = storeManager.getStore("store1");
            Store.TransactionBuilder transaction1_1 = store1.begin();
            transaction1_1.addCommand(CreatePerson.class).
                    set(CreatePerson::id).to("myPersonId1_2").
                    set(CreatePerson::name).to("myPersonName1_2").
                    submit();
            transaction1_1.submit().get();

            Store store2 = storeManager.getStore("store2");
            Store.TransactionBuilder transaction2_1 = store2.begin();
            transaction2_1.addCommand(CreatePerson.class).
                    set(CreatePerson::id).to("myPersonId2_2").
                    set(CreatePerson::name).to("myPersonName2_2").
                    submit();
            transaction2_1.submit().get();

            List<String> store1PersonIds = store1.query(tx -> {
                return tx.select(Person.class).all();
            }).stream().map(Person::id).collect(toList());
            assertThat(store1PersonIds).containsExactlyInAnyOrder("myPersonId1_1", "myPersonId1_2");

            List<String> store2PersonIds = store2.query(tx -> {
                return tx.select(Person.class).all();
            }).stream().map(Person::id).collect(toList());
            assertThat(store2PersonIds).containsExactlyInAnyOrder("myPersonId2_1", "myPersonId2_2");
        }

        // - I corrupt the Iron database by removing the last transaction in the store2
        Files.delete(filePath.resolve(directory).resolve("tx").resolve("00000000000000000003_store2.tx"));

        // - I try to open the store after removing the last transaction
        StoreManagerBuilder factoryBuilder3 = StoreManagerBuilder.newStoreManagerBuilder() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStore(transactionStoreFactory.get()) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStore(snapshotStoreFactory.get()) //
                .withEntityClass(Company.class) //
                .withEntityClass(Person.class) //
                .withCommandClass(CreatePerson.class) //
                ;
        try (StoreManager storeManager = factoryBuilder3.build()) {
            Store store1 = storeManager.getStore("store1");
            Store.TransactionBuilder transaction1_1 = store1.begin();
            transaction1_1.addCommand(CreatePerson.class).
                    set(CreatePerson::id).to("myPersonId1_3").
                    set(CreatePerson::name).to("myPersonName1_3").
                    submit();
            transaction1_1.submit().get();

            Store store2 = storeManager.getStore("store2");
            Store.TransactionBuilder transaction2_1 = store2.begin();
            transaction2_1.addCommand(CreatePerson.class).
                    set(CreatePerson::id).to("myPersonId2_3").
                    set(CreatePerson::name).to("myPersonName2_3").
                    submit();
            transaction2_1.submit().get();

            // Then
            // - the store1 contains the 3 transactions
            List<String> store1PersonIds = store1.query(tx -> {
                return tx.select(Person.class).all();
            }).stream().map(Person::id).collect(toList());
            assertThat(store1PersonIds).containsExactlyInAnyOrder("myPersonId1_1", "myPersonId1_2", "myPersonId1_3");

            // - the store2 has loss the 2nd transaction
            List<String> store2PersonIds = store2.query(tx -> {
                return tx.select(Person.class).all();
            }).stream().map(Person::id).collect(toList());
            assertThat(store2PersonIds).containsExactlyInAnyOrder("myPersonId2_1", "myPersonId2_3");
        }
    }
}


