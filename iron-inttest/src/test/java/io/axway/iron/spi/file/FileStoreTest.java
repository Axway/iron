package io.axway.iron.spi.file;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import org.testng.annotations.Test;
import io.axway.iron.ReadOnlyTransaction;
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
                .withCommandClass(ChangeCompanyAddress.class) //
                .withCommandClass(CreateCompany.class) //
                .withCommandClass(CreatePerson.class) //
                .withCommandClass(DeleteCompany.class) //
                .withCommandClass(PersonJoinCompany.class) //
                .withCommandClass(PersonLeaveCompany.class) //
                .withCommandClass(PersonRaiseSalary.class);

        Consumer<ReadOnlyTransaction> listInstances = tx -> {
            System.out.printf("Persons:%n");
            tx.select(Person.class).all().forEach(person -> {
                Company company = person.worksAt();
                System.out.printf("  %s currently works in %s%n", person, company != null ? company.name() : "<unemployed>");
                person.previousCompanies().forEach(previousCompany -> System.out.printf("     previously works in %s%n", previousCompany.name()));
            });
            System.out.printf("%n");

            tx.select(Company.class).all().forEach(company -> {
                System.out.printf("Company %s%n", company.name());
                company.employees().forEach(person -> System.out.printf("    employee %s%n", person.name()));
                company.previousEmployees().forEach(person -> System.out.printf("    previous employee %s%n", person.name()));
            });
        };

        try (StoreManager storeManager = factoryBuilder.build()) {
            Store store1 = storeManager.getStore("store1");
            //store1.query(listInstances);
            Store store2 = storeManager.getStore("store2");
            storeManager.snapshot();
            Store.TransactionBuilder transaction1_1 = store1.begin();
            transaction1_1.addCommand(CreatePerson.class).
                    set(CreatePerson::id).to("rolland").
                    set(CreatePerson::name).to("Rolland").
                    submit();
            //store1.query(listInstances);
            List<Object> result1_1 = transaction1_1.submit().get();

            store1.query(tx -> {
                Collection<Person> persons = tx.select(Person.class).all();
                System.out.printf("Person: %s%n", persons);
            });
            store1.query(listInstances);
            storeManager.snapshot();
        }
        List<Path> expectedPaths = new ArrayList<>();
        expectedPaths.add(filePath.resolve(directory).resolve("snapshot"));
        for (String snapshot : new String[]{"00000000000000000000", "00000000000000000001"}) {
            expectedPaths.add(filePath.resolve(directory).resolve("snapshot").resolve(snapshot));
            for (String store : new String[]{"store1", "store2"}) {
                expectedPaths.add(filePath.resolve(directory).resolve("snapshot").resolve(snapshot).resolve(store + ".snapshot"));
            }
        }
        assertThat(Files.walk(filePath.resolve(directory).resolve("snapshot")).collect(toSet())).
                containsExactlyInAnyOrderElementsOf(expectedPaths);
    }
}


