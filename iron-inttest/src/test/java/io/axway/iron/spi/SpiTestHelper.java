package io.axway.iron.spi;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import javax.annotation.*;
import org.assertj.core.api.Assertions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.axway.iron.Command;
import io.axway.iron.ReadOnlyTransaction;
import io.axway.iron.ReadWriteTransaction;
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

import static com.google.common.collect.ImmutableMap.*;
import static java.util.stream.Collectors.*;

public class SpiTestHelper {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd/mm/yyyy");

    @SuppressWarnings("unchecked")
    public static void checkThatCreateCompanySequenceIsRight(Supplier<TransactionStore> transactionStoreFactory, TransactionSerializer transactionSerializer,
                                                             Supplier<SnapshotStore> snapshotStoreFactory, SnapshotSerializer snapshotSerializer, String storeName)
            throws Exception {
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

        Consumer<ReadOnlyTransaction> checkData = tx -> {
            Person bill = tx.select(Person.class).where(Person::id).equalsTo("123");
            Company billCompany = bill.worksAt();
            Assertions.assertThat(billCompany).isNotNull();
            Assertions.assertThat(bill.birthDate()).isNotNull().isEqualToIgnoringHours("1990-01-01");
            System.out.printf("Query6: %s%n", billCompany.name() + " @ " + billCompany.address());
        };

        try (StoreManager factory = factoryBuilder.build()) {
            Store store = factory.getStore(storeName);

            store.query(listInstances);

            Store.TransactionBuilder tx1 = store.begin();
            tx1.addCommand(CreateCompany.class).set(CreateCompany::name).to("Google").set(CreateCompany::address).to("Palo Alto").submit();
            tx1.addCommand(CreateCompany.class).set(CreateCompany::name).to("Microsoft").set(CreateCompany::address).to("Seattle").submit();
            tx1.addCommand(CreateCompany.class).set(CreateCompany::name).to("Axway").set(CreateCompany::address).to("Phoenix").submit();
            tx1.addCommand(CreateCompany.class).map(of("name", "Apple", "address", "Cupertino")).submit();
            List<?> result = tx1.submit().get();
            Assertions.assertThat(result.size()).isEqualTo(4);
            Assertions.assertThat(result.get(0)).isEqualTo(0L);
            Assertions.assertThat(result.get(1)).isEqualTo(1L);
            Assertions.assertThat(result.get(2)).isEqualTo(2L);
            Assertions.assertThat(result.get(3)).isEqualTo(3L);

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

            factory.snapshot();
        }

        factoryBuilder = StoreManagerBuilder.newStoreManagerBuilder() //
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

        try (StoreManager factory = factoryBuilder.build()) {
            Store store = factory.getStore(storeName);
            store.query(listInstances);
            store.query(checkData);
        }
    }

    public static void checkThatListSnapshotsReturnTheRightNumberOfSnapshots(TransactionStore transactionStore,
                                                                             TransactionSerializer transactionSerializer,
                                                                             SnapshotStore snapshotStore, SnapshotSerializer snapshotSerializer,
                                                                             String storeName) throws Exception {
        System.out.println("storeManagerFactory1");

        StoreManagerBuilder factoryBuilder = StoreManagerBuilder.newStoreManagerBuilder() //
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
                .withCommandClass(PersonRaiseSalary.class);

        try (StoreManager factory = factoryBuilder //
                .build()) {
            Store store = factory.getStore(storeName);
            Assertions.assertThat(snapshotStore.listSnapshots()).hasSize(1);
            Store.TransactionBuilder tx1 = store.begin();
            tx1.addCommand(CreateCompany.class).set(CreateCompany::name).to("MyCompany1").submit();
            tx1.submit().get();
            Assertions.assertThat(snapshotStore.listSnapshots()).hasSize(1);
            factory.snapshot();
            Assertions.assertThat(snapshotStore.listSnapshots()).hasSize(2);
        }
    }

    public interface ExecutionCountCommand extends Command<Void> {

        int[] s_executionCount = new int[]{0};

        @Override
        default Void execute(@Nonnull ReadWriteTransaction tx) {
            s_executionCount[0] = s_executionCount[0] + 1;
            return null;
        }
    }

    public static void checkThatCommandIsExecutedFromSnapshotStoreNotFromTransactionStore(Supplier<TransactionStore> transactionStoreFactory,
                                                                                          TransactionSerializer transactionSerializer,
                                                                                          Supplier<SnapshotStore> snapshotStoreFactory,
                                                                                          SnapshotSerializer snapshotSerializer, String storeName)
            throws Exception {
        ExecutionCountCommand.s_executionCount[0] = 0;

        StoreManagerBuilder factoryBuilder = StoreManagerBuilder.newStoreManagerBuilder() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStore(transactionStoreFactory.get()) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStore(snapshotStoreFactory.get()) //
                .withCommandClass(ExecutionCountCommand.class);

        try (StoreManager storeManager = factoryBuilder.build()) {
            Store store1 = storeManager.getStore(storeName);
            Store.TransactionBuilder tx1 = store1.begin();
            tx1.addCommand(ExecutionCountCommand.class).submit();
            tx1.addCommand(ExecutionCountCommand.class).submit();
            Assertions.assertThat(ExecutionCountCommand.s_executionCount[0]).as("The store is empty, no command execution should have occurred.").isEqualTo(0);
            tx1.submit().get();
            Assertions.assertThat(ExecutionCountCommand.s_executionCount[0]).as("Two commands execution should have occurred : 0 + 2 = 2").isEqualTo(2);
            storeManager.snapshot();
            Store.TransactionBuilder tx2 = store1.begin();
            tx2.addCommand(ExecutionCountCommand.class).submit();
            tx2.addCommand(ExecutionCountCommand.class).submit();
            tx2.addCommand(ExecutionCountCommand.class).submit();
            Assertions.assertThat(ExecutionCountCommand.s_executionCount[0]).as("No command execution should have occurred since the last check.").isEqualTo(2);
            tx2.submit().get();
            Assertions.assertThat(ExecutionCountCommand.s_executionCount[0]).as("Three additional commands execution should have occurred : 2 + 3 = 5.")
                    .isEqualTo(5);
        }

        Assertions.assertThat(ExecutionCountCommand.s_executionCount[0]).as("No command execution should have occurred since the last check.").isEqualTo(5);

        factoryBuilder = StoreManagerBuilder.newStoreManagerBuilder() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStore(transactionStoreFactory.get()) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStore(snapshotStoreFactory.get()) //
                .withCommandClass(ExecutionCountCommand.class);
        //noinspection EmptyTryBlock
        try (StoreManager ignored = factoryBuilder.build()) {
        }
        Assertions.assertThat(ExecutionCountCommand.s_executionCount[0])
                .as("Starting from the Snapshot, only the three commands execution of the Transaction should have occurred : 5 + 3 = 8."
                            + " Result value 10 means that all Transactions (even before Snapshot) have been processed.").isEqualTo(8);
    }

    //region Tools

    @SafeVarargs
    private static void checkCompanies(String title, Store store, ImmutableMap<Object, Object>... expectedCompanies) {
        store.query(tx -> {
            Collection<Company> companies = tx.select(Company.class).all();
            System.out.printf(title + ": %s%n", companies);
            Assertions.assertThat(companies.stream()//
                                          .map(company -> {
                                              Builder<Object, Object> map = builder().put("name", company.name())
                                                      .put("address", Objects.requireNonNull(company.address()));
                                              String country = company.country();
                                              if (country != null) {
                                                  map.put("country", country);
                                              }
                                              return map.build();
                                          }).collect(toList())).containsExactlyInAnyOrder(expectedCompanies);
        });
    }

    @SafeVarargs
    private static void checkPersons(String title, Store store, ImmutableMap<Object, Object>... expectedPersons) {
        store.query(tx -> {
            Collection<Person> persons = tx.select(Person.class).all();
            System.out.printf(title + ": %s%n", persons);
            Assertions.assertThat(persons.stream()//
                                          .map(person -> {
                                              Builder<Object, Object> map = builder().put("name", person.name()).put("id", person.id());
                                              Date birthDate = person.birthDate();
                                              if (birthDate != null) {
                                                  map.put("birthDate", birthDate);
                                              }
                                              Double salary = person.salary();
                                              if (salary != null) {
                                                  map.put("salary", salary);
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
                Assertions.assertThat(e.getCause().getMessage()).as("An exception has correctly been thrown, but the message was not correct.")
                        .isEqualTo(expectedMessage);
                return;
            }
            Assertions.fail("No exception has been thrown, while exception with message [" + expectedMessage + "] was expected.");
        }
    }

    //endregion
}
