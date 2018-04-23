package io.axway.iron.spi.kafka;

import java.nio.file.Path;
import java.util.*;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import io.axway.iron.Store;
import io.axway.iron.StoreManager;
import io.axway.iron.StoreManagerFactory;
import io.axway.iron.core.spi.file.FileSnapshotStoreFactoryBuilder;
import io.axway.iron.sample.command.ChangeCompanyAddress;
import io.axway.iron.sample.command.CreateCompany;
import io.axway.iron.sample.command.CreatePerson;
import io.axway.iron.sample.command.DeleteCompany;
import io.axway.iron.sample.command.PersonJoinCompany;
import io.axway.iron.sample.command.PersonLeaveCompany;
import io.axway.iron.sample.command.PersonRaiseSalary;
import io.axway.iron.sample.model.Company;
import io.axway.iron.sample.model.Person;
import io.axway.iron.spi.jackson.JacksonSnapshotSerializerBuilder;
import io.axway.iron.spi.jackson.JacksonTransactionSerializerBuilder;

import static com.google.common.collect.ImmutableMap.of;
import static io.axway.iron.assertions.Assertions.assertThat;
import static io.axway.iron.core.StoreManagerFactoryBuilder.newStoreManagerBuilderFactory;
import static io.axway.iron.spi.kafka.Utils.*;
import static java.nio.file.Files.createTempDirectory;
import static java.util.UUID.*;
import static java.util.function.Function.*;
import static java.util.stream.Collectors.*;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * This class tests the Kafka SPI implementation with a single Kafka node
 */
@SuppressWarnings("Duplicates")
public class SingleKafkaSpiTest {
    private KafkaCluster m_kafkaCluster;
    private Path m_ironPath;
    private StoreManagerFactory m_storeManagerFactory;

    @BeforeClass
    public void setUp() throws Exception {
        // Start a new Kafka cluster
        m_kafkaCluster = KafkaCluster.createStarted(1);

        m_ironPath = createTempDirectory("iron-kafka-spi-");

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, m_kafkaCluster.getConnectionString());
        m_storeManagerFactory = newStoreManagerBuilderFactory().
                withSnapshotSerializer(new JacksonSnapshotSerializerBuilder().get()).
                withSnapshotStoreFactory((new FileSnapshotStoreFactoryBuilder()).setDir(m_ironPath).get()).
                withTransactionSerializer(new JacksonTransactionSerializerBuilder().get()).
                withTransactionStoreFactory((new KafkaTransactionStoreFactoryBuilder()).setProperties(kafkaProperties).get()).
                withEntityClass(Company.class).
                withEntityClass(Person.class).
                withCommandClass(ChangeCompanyAddress.class).
                withCommandClass(CreateCompany.class).
                withCommandClass(CreatePerson.class).
                withCommandClass(DeleteCompany.class).
                withCommandClass(PersonJoinCompany.class).
                withCommandClass(PersonLeaveCompany.class).
                withCommandClass(PersonRaiseSalary.class).
                build();
    }

    @AfterClass
    public void tearDown() {
        tryDeleteDirectory(m_ironPath);
        m_kafkaCluster.close();
    }

    @Test
    public final void shouldOpenNewStore() {
        // When opening a new store
        try (StoreManager storeManager = m_storeManagerFactory.openStore("shouldOpenNewStore-" + randomUUID())) {
            // Then it's possible and there's nothing in it
            storeManager.getStore().query(q -> {
                assertThat(q.select(Company.class).all()).isEmpty();
            });
        }
    }

    @Test
    public final void shouldInsertCompanies() {
        // Given a store
        try (StoreManager storeManager = m_storeManagerFactory.openStore("shouldInsertCompanies" + randomUUID())) {
            // When inserting 4 companies in one transaction
            Store.TransactionBuilder transaction = storeManager.getStore().begin();
            transaction.addCommand(CreateCompany.class).set(CreateCompany::name).to("Google").set(CreateCompany::address).to("Palo Alto").submit();
            transaction.addCommand(CreateCompany.class).set(CreateCompany::name).to("Microsoft").set(CreateCompany::address).to("Seattle").submit();
            transaction.addCommand(CreateCompany.class).set(CreateCompany::name).to("Axway").set(CreateCompany::address).to("Phoenix").submit();
            transaction.addCommand(CreateCompany.class).map(of("name", "Apple", "address", "Cupertino")).submit();
            List<Object> result = futureGet(transaction.submit());

            // Then they we are inserted and their identifiers are in ascending order
            assertThat(result).containsExactly(0L, 1L, 2L, 3L);

            // And we can retrieve them with queries
            storeManager.getStore().query(q -> {
                Map<Long, Company> companies = q.select(Company.class).all().stream().collect(toMap(Company::id, identity()));
                assertThat(companies).hasSize(4);
                assertThat(companies.get(0L)).hasName("Google").hasAddress("Palo Alto");
                assertThat(companies.get(1L)).hasName("Microsoft").hasAddress("Seattle");
                assertThat(companies.get(2L)).hasName("Axway").hasAddress("Phoenix");
                assertThat(companies.get(3L)).hasName("Apple").hasAddress("Cupertino");
            });
        }
    }

    @Test
    public final void shouldUpdateCompany() {
        // Given a store
        try (StoreManager storeManager = m_storeManagerFactory.openStore("shouldUpdateCompany-" + randomUUID())) {
            // And a company in the database
            futureGet(storeManager.getStore().createCommand(CreateCompany.class).
                    set(CreateCompany::name).to("Axway").
                    set(CreateCompany::address).to("Phoenix").
                    submit());

            // When updating it
            futureGet(storeManager.getStore().createCommand(ChangeCompanyAddress.class).
                    set(ChangeCompanyAddress::name).to("Axway").
                    set(ChangeCompanyAddress::newAddress).to("Puteaux").
                    set(ChangeCompanyAddress::newCountry).to("France").
                    submit());

            // Then it has the proper values
            storeManager.getStore().query(q -> {
                Company axway = q.select(Company.class).where(Company::name).equalsTo("Axway");
                assertThat(axway).hasName("Axway").hasAddress("Puteaux").hasCountry("France");
            });
        }
    }

    @Test
    public final void shouldDeleteCompany() {
        // Given a store
        try (StoreManager storeManager = m_storeManagerFactory.openStore("shouldDeleteCompany-" + randomUUID())) {
            // And a company in the database
            futureGet(storeManager.getStore().createCommand(CreateCompany.class).
                    set(CreateCompany::name).to("Axway").
                    set(CreateCompany::address).to("Phoenix").
                    submit());

            // When deleting it
            futureGet(storeManager.getStore().createCommand(DeleteCompany.class).
                    set(DeleteCompany::name).to("Axway").
                    submit());

            // Then it's deleted and the store is empty again
            storeManager.getStore().query(q -> {
                assertThat(q.select(Company.class).all()).isEmpty();
            });
        }
    }

    @Test
    public final void shouldResumeFromSnapshot() {
        String storeName = "shouldResumeFromSnapshot" + randomUUID();

        // Given a store
        try (StoreManager storeManager = m_storeManagerFactory.openStore(storeName)) {
            // When inserting 4 companies in one transaction
            Store.TransactionBuilder transaction = storeManager.getStore().begin();
            transaction.addCommand(CreateCompany.class).set(CreateCompany::name).to("Google").set(CreateCompany::address).to("Palo Alto").submit();
            transaction.addCommand(CreateCompany.class).set(CreateCompany::name).to("Microsoft").set(CreateCompany::address).to("Seattle").submit();
            transaction.addCommand(CreateCompany.class).set(CreateCompany::name).to("Axway").set(CreateCompany::address).to("Phoenix").submit();
            transaction.addCommand(CreateCompany.class).map(of("name", "Apple", "address", "Cupertino")).submit();
            futureGet(transaction.submit());

            // When creating a snapshot
            storeManager.snapshot();
        }

        // Then we can reopen the store and query the companies
        try (StoreManager storeManager = m_storeManagerFactory.openStore(storeName)) {
            storeManager.getStore().query(q -> {
                Map<Long, Company> companies = q.select(Company.class).all().stream().collect(toMap(Company::id, identity()));
                assertThat(companies).hasSize(4);
                assertThat(companies.get(0L)).hasName("Google").hasAddress("Palo Alto");
                assertThat(companies.get(1L)).hasName("Microsoft").hasAddress("Seattle");
                assertThat(companies.get(2L)).hasName("Axway").hasAddress("Phoenix");
                assertThat(companies.get(3L)).hasName("Apple").hasAddress("Cupertino");
            });
        }
    }
}
