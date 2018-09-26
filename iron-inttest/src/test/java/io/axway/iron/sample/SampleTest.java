package io.axway.iron.sample;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

import static io.axway.iron.spi.chronicle.ChronicleTestHelper.buildChronicleTransactionStore;
import static io.axway.iron.spi.file.FileTestHelper.*;
import static io.axway.iron.spi.jackson.JacksonTestHelper.*;
import static java.util.UUID.*;
import static org.assertj.core.api.Assertions.assertThat;

public class SampleTest {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd/mm/yyyy");
    private Path storesFilePath;

    @AfterClass
    public void afterClass() throws IOException {
        if (storesFilePath.toFile().exists()) {
            Files.walk(storesFilePath).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
    }

    @DataProvider(name = "stores")
    public Object[][] providesStores() {
        storesFilePath = Paths.get("tmp-iron-test", "stores-" + randomUUID());

        SnapshotStore fileSnapshotStore1 = buildFileSnapshotStore(storesFilePath, "iron-sample-ss-file-1");
        SnapshotStore fileSnapshotStore2 = buildFileSnapshotStore(storesFilePath, "iron-sample-ss-file-2");
        TransactionStore fileTransactionStore = buildFileTransactionStore(storesFilePath, "iron-sample-tx-file-1");
        TransactionStore chronicleTransactionStore = buildChronicleTransactionStore(storesFilePath, "iron-sample-tx-cn-1");

        String storeBaseName = "irontest-" + System.getProperty("user.name");
        return new Object[][]{ //
                {"chronicleTx fileSs", chronicleTransactionStore, fileSnapshotStore1, storeBaseName + "-" + randomUUID()}, //
                {"fileTx fileSs", fileTransactionStore, fileSnapshotStore2, storeBaseName + "-" + randomUUID()}, //
        };
    }

    @Test(dataProvider = "stores")
    public void testCreateCompany(String message, TransactionStore transactionStore, SnapshotStore snapshotStore, String storeName) throws Exception {
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();

        try (StoreManager storeManager = StoreManagerBuilder.newStoreManagerBuilder() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStore(transactionStore) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStore(snapshotStore) //
                .withEntityClass(Company.class) //
                .withEntityClass(Person.class) //
                .withCommandClass(ChangeCompanyAddress.class) //
                .withCommandClass(CreateCompany.class) //
                .withCommandClass(CreatePerson.class) //
                .withCommandClass(DeleteCompany.class) //
                .withCommandClass(PersonJoinCompany.class) //
                .withCommandClass(PersonLeaveCompany.class) //
                .withCommandClass(PersonRaiseSalary.class) //
                .build()) {

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

            Consumer<ReadOnlyTransaction> checkData = tx -> {
                Person bill = tx.select(Person.class).where(Person::id).equalsTo("123");
                Company billCompany = bill.worksAt();
                assertThat(billCompany).isNotNull();
                assertThat(bill.birthDate()).isNotNull().isEqualToIgnoringHours("1990-01-01");
                System.out.printf("Query6: %s%n", billCompany.name() + " @ " + billCompany.address());
            };

            Store store = storeManager.getStore(storeName);
            store.query(listInstances);

            Store.TransactionBuilder tx1 = store.begin();
            tx1.addCommand(CreateCompany.class).set(CreateCompany::name).to("Google").set(CreateCompany::address).to("Palo Alto").submit();
            tx1.addCommand(CreateCompany.class).set(CreateCompany::name).to("Microsoft").set(CreateCompany::address).to("Seattle").submit();
            tx1.addCommand(CreateCompany.class).set(CreateCompany::name).to("Axway").set(CreateCompany::address).to("Phoenix").submit();
            tx1.addCommand(CreateCompany.class).map(ImmutableMap.of("name", "Apple", "address", "Cupertino")).submit();
            List<?> result = tx1.submit().get();
            assertThat(result.size()).isEqualTo(4);
            assertThat(result.get(0)).isEqualTo(0L);
            assertThat(result.get(1)).isEqualTo(1L);
            assertThat(result.get(2)).isEqualTo(2L);
            assertThat(result.get(3)).isEqualTo(3L);

            Future<Void> c1 = store.createCommand(ChangeCompanyAddress.class) //
                    .set(ChangeCompanyAddress::name).to("Apple") //
                    .set(ChangeCompanyAddress::newAddress).to("Cupertino") //
                    .set(ChangeCompanyAddress::newCountry).to("USA") //
                    .submit();
            awaitAllAndDiscardErrors(c1);

            store.query(tx -> {
                System.out.printf("Batch1: %s%n", tx.select(Company.class).all());
            });

            Future<Void> c4 = store.createCommand(DeleteCompany.class).set(DeleteCompany::name).to("Apple").submit();
            awaitAll(c4);

            store.query(tx -> {
                System.out.printf("Batch2: %s%n", tx.select(Company.class).all());
            });

            Future<Void> c5 = store.createCommand(ChangeCompanyAddress.class) //
                    .set(ChangeCompanyAddress::name).to("Google") //
                    .set(ChangeCompanyAddress::newAddress).to("Palo Alto") //
                    .set(ChangeCompanyAddress::newCountry).to("") //
                    .submit();

            Future<Long> c6 = store.createCommand(CreateCompany.class) //
                    .set(CreateCompany::name).to("Facebook") //
                    .set(CreateCompany::address).to("") //
                    .submit();

            Future<Void> c7 = store.createCommand(DeleteCompany.class) //
                    .set(DeleteCompany::name).to("Google") //
                    .submit();

            awaitAllAndDiscardErrors(c5, c6, c7);

            store.query(tx -> {
                System.out.printf("Batch3: %s%n", tx.select(Company.class).all());
            });

            Store.TransactionBuilder tx8 = store.begin();
            tx8.addCommand(CreatePerson.class) //
                    .set(CreatePerson::id).to("123") //
                    .set(CreatePerson::name).to("bill") //
                    .set(CreatePerson::previousCompanyNames).to(ImmutableList.of("Google", "Axway")) //
                    .set(CreatePerson::birthDate).to(DATE_FORMAT.parse("01/01/1990")) //
                    .submit();

            tx8.addCommand(CreatePerson.class) //
                    .set(CreatePerson::id).to("456") //
                    .set(CreatePerson::name).to("john") //
                    .submit();

            tx8.addCommand(CreatePerson.class) //
                    .set(CreatePerson::id).to("789") //
                    .set(CreatePerson::name).to("mark") //
                    .submit();

            awaitAll(tx8.submit());

            store.query(tx -> {
                System.out.printf("Batch4: %s%n", tx.select(Person.class).all());
            });

            Store.TransactionBuilder tx9 = store.begin();
            tx9.addCommand(PersonJoinCompany.class) //
                    .set(PersonJoinCompany::personId).to("123") //
                    .set(PersonJoinCompany::companyName).to("Microsoft") //
                    .set(PersonJoinCompany::salary).to(111111.0) //
                    .submit();
            tx9.addCommand(PersonJoinCompany.class) //
                    .set(PersonJoinCompany::personId).to("456") //
                    .set(PersonJoinCompany::companyName).to("Microsoft") //
                    .set(PersonJoinCompany::salary).to(100000.0) //
                    .submit();
            tx9.addCommand(PersonJoinCompany.class) //
                    .set(PersonJoinCompany::personId).to("789") //
                    .set(PersonJoinCompany::companyName).to("Google") //
                    .set(PersonJoinCompany::salary).to(123456.0) //
                    .submit();

            awaitAll(tx9.submit());

            store.query(tx -> {
                Collection<Person> persons = tx.select(Person.class).all();
                System.out.printf("Batch5: %s%n", persons);
            });

            store.query(checkData);
            storeManager.snapshot();

            store.query(listInstances);
            store.query(checkData);
        }
    }

    private void awaitAll(Future<?>... futures) throws ExecutionException, InterruptedException {
        for (Future<?> future : futures) {
            future.get();
        }
    }

    private void awaitAllAndDiscardErrors(Future<?>... futures) throws InterruptedException {
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                // discarded
                System.out.printf("%s%n", e.getCause().getMessage());
            }
        }
    }
}
