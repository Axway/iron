package io.axway.iron.core;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.*;
import org.assertj.core.api.Assertions;
import org.joda.time.DateTime;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import com.google.common.collect.ImmutableMap;
import io.axway.iron.ReadOnlyTransaction;
import io.axway.iron.Store;
import io.axway.iron.StoreManager;
import io.axway.iron.sample.command.CreateCompany;
import io.axway.iron.sample.command.CreatePerson;
import io.axway.iron.sample.command.MultipleRelationsAddAllTestCommand;
import io.axway.iron.sample.command.MultipleRelationsAddOneTestCommand;
import io.axway.iron.sample.command.MultipleRelationsClearTestCommand;
import io.axway.iron.sample.command.MultipleRelationsRemoveAllTestCommand;
import io.axway.iron.sample.command.MultipleRelationsRemoveOneTestCommand;
import io.axway.iron.sample.model.Company;
import io.axway.iron.sample.model.Person;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

import static io.axway.iron.spi.chronicle.ChronicleTestHelper.buildChronicleTransactionStoreFactory;
import static io.axway.iron.spi.file.FileTestHelper.*;
import static io.axway.iron.spi.jackson.JacksonTestHelper.*;

public class MultipleRelationsAPITest {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd/mm/yyyy");
    SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
    TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();

    StoreManager storeManager;

    @DataProvider(name = "stores")
    public Object[][] providesStores() {
        Path filePath = Paths.get("tmp-iron-test");
        SnapshotStore fileSnapshotStore = buildFileSnapshotStore(filePath, "iron-sample");
        TransactionStore fileTransactionStore = buildFileTransactionStore(filePath, "iron-sample");

        TransactionStore chronicleTransactionStore = buildChronicleTransactionStoreFactory("iron-sample", filePath);

        String storeBaseName = "irontest-" + System.getProperty("user.name");

        return new Object[][]{ //
                {chronicleTransactionStore, fileSnapshotStore, storeBaseName + "-" + UUID.randomUUID()}, //
                {fileTransactionStore, fileSnapshotStore, storeBaseName + "-" + UUID.randomUUID()}, //
        };
    }

    @Test(dataProvider = "stores")
    public void testAddOne(TransactionStore transactionStore, SnapshotStore snapshotStore, String storeName) throws Exception {

        StoreManager storeManager = StoreManagerBuilder.newStoreManagerBuilder() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStore(transactionStore) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStore(snapshotStore) //
                .withEntityClass(Company.class) //
                .withEntityClass(Person.class) //
                .withCommandClass(CreateCompany.class) //
                .withCommandClass(CreatePerson.class) //
                .withCommandClass(MultipleRelationsAddAllTestCommand.class) //
                .withCommandClass(MultipleRelationsAddOneTestCommand.class) //
                .withCommandClass(MultipleRelationsRemoveOneTestCommand.class) //
                .withCommandClass(MultipleRelationsRemoveAllTestCommand.class) //
                .withCommandClass(MultipleRelationsClearTestCommand.class) //
                .build();

        Store store = storeManager.getStore(storeName);

        Store.TransactionBuilder tx1 = store.begin();
        setUpCompanies(tx1);

        Store.TransactionBuilder tx2 = store.begin();
        setUpPersons(tx2);
        tx2.addCommand(MultipleRelationsAddOneTestCommand.class).set(MultipleRelationsAddOneTestCommand::personId).to("22").submit();
        tx2.submit().get();

        Function<ReadOnlyTransaction, Person> retrieveUpdatedPerson = (tx) -> {
            return tx.select(Person.class).where(Person::id).equalsToOrNull("22");
        };
        Person updatedPerson = store.query(retrieveUpdatedPerson);

        Assertions.assertThat(updatedPerson.previousCompanies().size()).isEqualTo(3);
        Assertions.assertThat(updatedPerson.previousCompanies().stream().findFirst().get().name()).isEqualTo("Google");
        Assertions.assertThat(updatedPerson.previousCompanies().stream().skip(1).findFirst().get().name()).isEqualTo("Microsoft");
        Assertions.assertThat(updatedPerson.previousCompanies().stream().skip(2).findFirst().get().name()).isEqualTo("Apple");
    }

    @Test(dataProvider = "stores")
    public void testAddAll(TransactionStore transactionStore, SnapshotStore snapshotStore, String storeName) throws Exception {

        StoreManager storeManager = StoreManagerBuilder.newStoreManagerBuilder() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStore(transactionStore) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStore(snapshotStore) //
                .withEntityClass(Company.class) //
                .withEntityClass(Person.class) //
                .withCommandClass(CreateCompany.class) //
                .withCommandClass(CreatePerson.class) //
                .withCommandClass(MultipleRelationsAddAllTestCommand.class) //
                .withCommandClass(MultipleRelationsAddOneTestCommand.class) //
                .withCommandClass(MultipleRelationsRemoveOneTestCommand.class) //
                .withCommandClass(MultipleRelationsRemoveAllTestCommand.class) //
                .withCommandClass(MultipleRelationsClearTestCommand.class) //
                .build();

        Store store = storeManager.getStore(storeName);

        Store.TransactionBuilder tx1 = store.begin();
        setUpCompanies(tx1);

        Store.TransactionBuilder tx2 = store.begin();
        setUpPersons(tx2);
        tx2.addCommand(MultipleRelationsAddAllTestCommand.class).set(MultipleRelationsAddAllTestCommand::personId).to("22").submit();
        tx2.submit().get();

        Function<ReadOnlyTransaction, Person> retrieveUpdatedPerson = (tx) -> {
            return tx.select(Person.class).where(Person::id).equalsToOrNull("22");
        };
        Person updatedPerson = store.query(retrieveUpdatedPerson);

        Assertions.assertThat(updatedPerson.previousCompanies().size()).isEqualTo(5);
        Assertions.assertThat(updatedPerson.previousCompanies().stream().findFirst().get().name()).isEqualTo("Google");
        Assertions.assertThat(updatedPerson.previousCompanies().stream().skip(1).findFirst().get().name()).isEqualTo("Microsoft");
        Assertions.assertThat(updatedPerson.previousCompanies().stream().skip(2).findFirst().get().name()).isEqualTo("Axway");
        Assertions.assertThat(updatedPerson.previousCompanies().stream().skip(3).findFirst().get().name()).isEqualTo("Oracle");
        Assertions.assertThat(updatedPerson.previousCompanies().stream().skip(4).findFirst().get().name()).isEqualTo("Apple");
    }

    @Test(dataProvider = "stores")
    public void testRemoveOne(TransactionStore transactionStore, SnapshotStore snapshotStore, String storeName) throws Exception {

        StoreManager storeManager = StoreManagerBuilder.newStoreManagerBuilder() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStore(transactionStore) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStore(snapshotStore) //
                .withEntityClass(Company.class) //
                .withEntityClass(Person.class) //
                .withCommandClass(CreateCompany.class) //
                .withCommandClass(CreatePerson.class) //
                .withCommandClass(MultipleRelationsAddAllTestCommand.class) //
                .withCommandClass(MultipleRelationsAddOneTestCommand.class) //
                .withCommandClass(MultipleRelationsRemoveOneTestCommand.class) //
                .withCommandClass(MultipleRelationsRemoveAllTestCommand.class) //
                .withCommandClass(MultipleRelationsClearTestCommand.class) //
                .build();

        Store store = storeManager.getStore(storeName);

        Store.TransactionBuilder tx1 = store.begin();
        setUpCompanies(tx1);

        Store.TransactionBuilder tx2 = store.begin();
        setUpPersonsWithSeveralPreviousCompanies(tx2);
        tx2.addCommand(MultipleRelationsRemoveOneTestCommand.class).set(MultipleRelationsRemoveOneTestCommand::personId).to("22").submit();
        tx2.submit().get();

        Function<ReadOnlyTransaction, Person> retrieveUpdatedPerson = (tx) -> {
            return tx.select(Person.class).where(Person::id).equalsToOrNull("22");
        };
        Person updatedPerson = store.query(retrieveUpdatedPerson);

        Assertions.assertThat(updatedPerson.previousCompanies().size()).isEqualTo(1);
        Assertions.assertThat(updatedPerson.previousCompanies().stream().findFirst().get().name()).isEqualTo("Apple");
    }

    @Test(dataProvider = "stores")
    public void testRemoveAll(TransactionStore transactionStore, SnapshotStore snapshotStore, String storeName) throws Exception {

        StoreManager storeManager = StoreManagerBuilder.newStoreManagerBuilder() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStore(transactionStore) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStore(snapshotStore) //
                .withEntityClass(Company.class) //
                .withEntityClass(Person.class) //
                .withCommandClass(CreateCompany.class) //
                .withCommandClass(CreatePerson.class) //
                .withCommandClass(MultipleRelationsAddAllTestCommand.class) //
                .withCommandClass(MultipleRelationsAddOneTestCommand.class) //
                .withCommandClass(MultipleRelationsRemoveOneTestCommand.class) //
                .withCommandClass(MultipleRelationsRemoveAllTestCommand.class) //
                .withCommandClass(MultipleRelationsClearTestCommand.class) //
                .build();

        Store store = storeManager.getStore(storeName);

        Store.TransactionBuilder tx1 = store.begin();
        setUpCompanies(tx1);

        Store.TransactionBuilder tx2 = store.begin();
        setUpPersonsWithSeveralPreviousCompanies(tx2);
        tx2.addCommand(MultipleRelationsRemoveAllTestCommand.class).set(MultipleRelationsRemoveAllTestCommand::personId).to("22").submit();
        tx2.submit().get();

        Function<ReadOnlyTransaction, Person> retrieveUpdatedPerson = (tx) -> {
            return tx.select(Person.class).where(Person::id).equalsToOrNull("22");
        };
        Person updatedPerson = store.query(retrieveUpdatedPerson);

        Assertions.assertThat(updatedPerson.previousCompanies().size()).isEqualTo(1);
        Assertions.assertThat(updatedPerson.previousCompanies().stream().findFirst().get().name()).isEqualTo("Apple");
    }

    @Test(dataProvider = "stores")
    public void testClear(TransactionStore transactionStore, SnapshotStore snapshotStore, String storeName) throws Exception {

        StoreManager storeManager = StoreManagerBuilder.newStoreManagerBuilder() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStore(transactionStore) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStore(snapshotStore) //
                .withEntityClass(Company.class) //
                .withEntityClass(Person.class) //
                .withCommandClass(CreateCompany.class) //
                .withCommandClass(CreatePerson.class) //
                .withCommandClass(MultipleRelationsAddAllTestCommand.class) //
                .withCommandClass(MultipleRelationsAddOneTestCommand.class) //
                .withCommandClass(MultipleRelationsRemoveOneTestCommand.class) //
                .withCommandClass(MultipleRelationsRemoveAllTestCommand.class) //
                .withCommandClass(MultipleRelationsClearTestCommand.class) //
                .build();

        Store store = storeManager.getStore(storeName);

        Store.TransactionBuilder tx1 = store.begin();
        setUpCompanies(tx1);

        Store.TransactionBuilder tx2 = store.begin();
        setUpPersons(tx2);
        tx2.addCommand(MultipleRelationsClearTestCommand.class).set(MultipleRelationsClearTestCommand::personId).to("22").submit();
        tx2.submit().get();

        Function<ReadOnlyTransaction, Person> retrieveUpdatedPerson = (tx) -> {
            return tx.select(Person.class).where(Person::id).equalsToOrNull("22");
        };
        Person updatedPerson = store.query(retrieveUpdatedPerson);

        Assertions.assertThat(updatedPerson.previousCompanies().size()).isEqualTo(0);
    }

    private void setUpCompanies(Store.TransactionBuilder tx) throws Exception {
        tx.addCommand(CreateCompany.class).set(CreateCompany::name).to("Google").set(CreateCompany::address).to("Palo Alto").submit();
        tx.addCommand(CreateCompany.class).set(CreateCompany::name).to("Microsoft").set(CreateCompany::address).to("Seattle").submit();
        tx.addCommand(CreateCompany.class).set(CreateCompany::name).to("Axway").set(CreateCompany::address).to("Phoenix").submit();
        tx.addCommand(CreateCompany.class).set(CreateCompany::name).to("Oracle").set(CreateCompany::address).to("Redwood City").submit();
        tx.addCommand(CreateCompany.class).map(ImmutableMap.of("name", "Apple", "address", "Cupertino")).submit();
        tx.submit();
    }

    private void setUpPersons(Store.TransactionBuilder tx) throws Exception {
        tx.addCommand(CreatePerson.class).set(CreatePerson::id).to("11").set(CreatePerson::name).to("Marcel").set(CreatePerson::previousCompanyNames)
                .to(Arrays.asList("Google", "Microsoft")).set(CreatePerson::birthDate).to(new DateTime().minusYears(20).toDate()).submit();
        tx.addCommand(CreatePerson.class).set(CreatePerson::id).to("22").set(CreatePerson::name).to("Sinclair").set(CreatePerson::worksAt).to("Axway")
                .set(CreatePerson::previousCompanyNames).to(Arrays.asList("Apple")).set(CreatePerson::birthDate).to(new DateTime().minusYears(30).toDate())
                .submit();
    }

    private void setUpPersonsWithSeveralPreviousCompanies(Store.TransactionBuilder tx) throws Exception {
        tx.addCommand(CreatePerson.class).set(CreatePerson::id).to("11").set(CreatePerson::name).to("Marcel").set(CreatePerson::previousCompanyNames)
                .to(Arrays.asList("Google", "Microsoft")).set(CreatePerson::birthDate).to(new DateTime().minusYears(20).toDate()).submit();
        tx.addCommand(CreatePerson.class).set(CreatePerson::id).to("22").set(CreatePerson::name).to("Sinclair").set(CreatePerson::worksAt).to("Axway")
                .set(CreatePerson::previousCompanyNames).to(Arrays.asList("Apple", "Oracle", "Google")).set(CreatePerson::birthDate)
                .to(new DateTime().minusYears(30).toDate()).submit();
    }
}
