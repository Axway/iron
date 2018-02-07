package io.axway.iron.sample;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import javax.annotation.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.axway.iron.Command;
import io.axway.iron.ReadOnlyTransaction;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.Store;
import io.axway.iron.StoreManager;
import io.axway.iron.StoreManagerFactory;
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
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static com.google.common.collect.ImmutableMap.*;
import static io.axway.iron.core.StoreManagerFactoryBuilder.newStoreManagerBuilderFactory;
import static java.util.stream.Collectors.*;
import static org.assertj.core.api.Assertions.*;

public class Sample {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd/mm/yyyy");

    public static void checkThatCreateCompanySequenceIsRight(TransactionStoreFactory transactionStoreFactory, TransactionSerializer transactionSerializer,
                                                             SnapshotStoreFactory snapshotStoreFactory, SnapshotSerializer snapshotSerializer, String storeName)
            throws Exception {
        StoreManagerFactory storeManagerFactory = newStoreManagerBuilderFactory() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStoreFactory(transactionStoreFactory) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStoreFactory(snapshotStoreFactory) //
                .withEntityClass(Company.class) //
                .withEntityClass(Person.class) //
                .withCommandClass(ChangeCompanyAddress.class) //
                .withCommandClass(CreateCompany.class) //
                .withCommandClass(CreatePerson.class) //
                .withCommandClass(DeleteCompany.class) //
                .withCommandClass(PersonJoinCompany.class) //
                .withCommandClass(PersonLeaveCompany.class) //
                .withCommandClass(PersonRaiseSalary.class) //
                .build();

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

        try (StoreManager storeManager = storeManagerFactory.openStore(storeName)) {

            Store store = storeManager.getStore();

            store.query(listInstances);

            Store.TransactionBuilder tx1 = store.begin();
            tx1.addCommand(CreateCompany.class).set(CreateCompany::name).to("Google").set(CreateCompany::address).to("Palo Alto").submit();
            tx1.addCommand(CreateCompany.class).set(CreateCompany::name).to("Microsoft").set(CreateCompany::address).to("Seattle").submit();
            tx1.addCommand(CreateCompany.class).set(CreateCompany::name).to("Axway").set(CreateCompany::address).to("Phoenix").submit();
            tx1.addCommand(CreateCompany.class).map(of("name", "Apple", "address", "Cupertino")).submit();
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

            awaitAndCheckMessage(c1, null);

            checkCompanies("Batch1", store,//
                           of("name", "Google", "address", "Palo Alto"),//
                           of("name", "Microsoft", "address", "Seattle"),//
                           of("name", "Axway", "address", "Phoenix"),//
                           of("name", "Apple", "address", "Cupertino", "country", "USA")//
            );

            Future<Void> c4 = store.createCommand(DeleteCompany.class).set(DeleteCompany::name).to("Apple").submit();

            awaitAndCheckMessage(c4, null);

            checkCompanies("Batch2", store,//
                           of("name", "Google", "address", "Palo Alto"),//
                           of("name", "Microsoft", "address", "Seattle"),//
                           of("name", "Axway", "address", "Phoenix")//
            );

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

            awaitAndCheckMessage(c5, "New country must be null or not empty");
            awaitAndCheckMessage(c6, "New address must be null or not empty");
            awaitAndCheckMessage(c7, "You cannot delete Google!");

            checkCompanies("Batch3", store,//
                           of("name", "Google", "address", "Palo Alto"),//
                           of("name", "Microsoft", "address", "Seattle"),//
                           of("name", "Axway", "address", "Phoenix")//
            );

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

            awaitAndCheckMessage(tx8.submit(), null);

            checkPersons("Batch4", store, of("name", "bill", "id", "123", "birthDate", DATE_FORMAT.parse("01/01/1990")),//
                         of("name", "john", "id", "456"),//
                         of("name", "mark", "id", "789")//
            );

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

            awaitAndCheckMessage(tx9.submit(), null);

            checkPersons("Batch5", store, of("name", "bill", "id", "123", "salary", 111_111., "birthDate", DATE_FORMAT.parse("01/01/1990")),//
                         of("name", "john", "id", "456", "salary", 100_000.),//
                         of("name", "mark", "id", "789", "salary", 123_456.)//
            );

            store.query(checkData);

            storeManager.snapshot();
        }

        try (StoreManager storeManager = storeManagerFactory.openStore(storeName)) {
            Store store = storeManager.getStore();
            store.query(listInstances);
            store.query(checkData);
        }
    }

    public static void checkThatListSnapshotsReturnTheRightNumberOfSnapshots(TransactionStoreFactory transactionStoreFactory,
                                                                             TransactionSerializer transactionSerializer,
                                                                             SnapshotStoreFactory snapshotStoreFactory, SnapshotSerializer snapshotSerializer,
                                                                             String storeName) throws Exception {
        System.out.println("storeManagerFactory1");

        StoreManagerFactory storeManagerFactory1 = newStoreManagerBuilderFactory() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStoreFactory(transactionStoreFactory) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStoreFactory(snapshotStoreFactory) //
                .withEntityClass(Company.class) //
                .withEntityClass(Person.class) //
                .withCommandClass(ChangeCompanyAddress.class) //
                .withCommandClass(CreateCompany.class) //
                .withCommandClass(CreatePerson.class) //
                .withCommandClass(DeleteCompany.class) //
                .withCommandClass(PersonJoinCompany.class) //
                .withCommandClass(PersonLeaveCompany.class) //
                .withCommandClass(PersonRaiseSalary.class) //
                .build();

        try (StoreManager storeManager1 = storeManagerFactory1.openStore(storeName)) {
            assertThat(snapshotStoreFactory.createSnapshotStore(storeName).listSnapshots()).hasSize(1);
            Store store1 = storeManager1.getStore();
            Store.TransactionBuilder tx1 = store1.begin();
            tx1.addCommand(CreateCompany.class).set(CreateCompany::name).to("MyCompany1").submit();
            tx1.submit().get();
            assertThat(snapshotStoreFactory.createSnapshotStore(storeName).listSnapshots()).hasSize(1);
            storeManager1.snapshot();
            assertThat(snapshotStoreFactory.createSnapshotStore(storeName).listSnapshots()).hasSize(2);
        }
    }

    public interface ExecutionCountCommand extends Command<Void> {

        int[] s_executionCount = new int[]{0};

        @Override
        default Void execute(ReadWriteTransaction tx) {
            s_executionCount[0] = s_executionCount[0] + 1;
            return null;
        }
    }

    public static void checkThatCommandIsExecutedFromSnapshotStoreNotFromTransactionStore(TransactionStoreFactory transactionStoreFactory,
                                                                                          TransactionSerializer transactionSerializer,
                                                                                          SnapshotStoreFactory snapshotStoreFactory,
                                                                                          SnapshotSerializer snapshotSerializer, String storeName)
            throws Exception {
        ExecutionCountCommand.s_executionCount[0] = 0;

        StoreManagerFactory storeManagerFactory1 = newStoreManagerBuilderFactory() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStoreFactory(transactionStoreFactory) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStoreFactory(snapshotStoreFactory) //
                .withCommandClass(ExecutionCountCommand.class) //
                .build();

        try (StoreManager storeManager1 = storeManagerFactory1.openStore(storeName)) {
            Store store1 = storeManager1.getStore();
            Store.TransactionBuilder tx1 = store1.begin();
            tx1.addCommand(ExecutionCountCommand.class).submit();
            tx1.addCommand(ExecutionCountCommand.class).submit();
            assertThat(ExecutionCountCommand.s_executionCount[0]).as("The store is empty, no command execution should have occurred.").isEqualTo(0);
            tx1.submit().get();
            assertThat(ExecutionCountCommand.s_executionCount[0]).as("Two commands execution should have occurred : 0 + 2 = 2").isEqualTo(2);
            storeManager1.snapshot();
            Store.TransactionBuilder tx2 = store1.begin();
            tx2.addCommand(ExecutionCountCommand.class).submit();
            tx2.addCommand(ExecutionCountCommand.class).submit();
            tx2.addCommand(ExecutionCountCommand.class).submit();
            assertThat(ExecutionCountCommand.s_executionCount[0]).as("No command execution should have occurred since the last check.").isEqualTo(2);
            tx2.submit().get();
            assertThat(ExecutionCountCommand.s_executionCount[0]).as("Three additional commands execution should have occurred : 2 + 3 = 5.").isEqualTo(5);
        }

        StoreManagerFactory storeManagerFactory2 = newStoreManagerBuilderFactory() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStoreFactory(transactionStoreFactory) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStoreFactory(snapshotStoreFactory) //
                .withCommandClass(ExecutionCountCommand.class) //
                .build();

        assertThat(ExecutionCountCommand.s_executionCount[0]).as("No command execution should have occurred since the last check.").isEqualTo(5);
        try (StoreManager storeManager2 = storeManagerFactory2.openStore(storeName)) {
        }
        assertThat(ExecutionCountCommand.s_executionCount[0])
                .as("Starting from the Snapshot, only the three commands execution of the Transaction should have occurred : 5 + 3 = 8."
                            + " Result value 10 means that all Transactions (even before Snapshot) have been processed.").isEqualTo(8);
    }

    //region Tools

    private static void checkCompanies(String title, Store store, ImmutableMap<Object, Object>... expectedCompanies) {
        store.query(tx -> {
            Collection<Company> companies = tx.select(Company.class).all();
            System.out.printf(title + ": %s%n", companies);
            assertThat(companies.stream()//
                               .map(company -> {
                                   Builder<Object, Object> map = builder().put("name", company.name()).put("address", company.address());
                                   if (company.country() != null) {
                                       map.put("country", company.country());
                                   }
                                   return map.build();
                               }).collect(toList())).containsExactlyInAnyOrder(expectedCompanies);
        });
    }

    private static void checkPersons(String title, Store store, ImmutableMap<Object, Object>... expectedPersons) {
        store.query(tx -> {
            Collection<Person> persons = tx.select(Person.class).all();
            System.out.printf(title + ": %s%n", persons);
            assertThat(persons.stream()//
                               .map(person -> {
                                   Builder<Object, Object> map = builder().put("name", person.name()).put("id", person.id());
                                   if (person.birthDate() != null) {
                                       map.put("birthDate", person.birthDate());
                                   }
                                   if (person.salary() != null) {
                                       map.put("salary", person.salary());
                                   }
                                   return map.build();
                               }).collect(toList())).containsExactlyInAnyOrder(expectedPersons);
        });
    }

    private static void awaitAndCheckMessage(Future<?> future, @Nullable String expectedMessage) throws InterruptedException, ExecutionException {
        if (expectedMessage == null) {
            future.get();
        } else {
            try {
                future.get();
            } catch (ExecutionException e) {
                // discarded
                assertThat(e.getCause().getMessage()).as("An exception has corretly been thrown, but the message was not correct.").isEqualTo(expectedMessage);
                return;
            }
            fail("No exception has been thrown, while exception with message [" + expectedMessage + "] was expected.");
        }
    }

    //endregion
}
