package io.axway.iron.spi.kafka;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import io.axway.iron.Store;
import io.axway.iron.StoreManager;
import io.axway.iron.core.StoreManagerBuilder;
import io.axway.iron.core.spi.file.FileSnapshotStoreBuilder;
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

import static io.axway.iron.assertions.Assertions.assertThat;
import static io.axway.iron.spi.Utils.*;
import static java.nio.file.Files.createTempDirectory;
import static java.util.UUID.*;
import static java.util.concurrent.TimeUnit.*;
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
    private StoreManager m_storeManager;

    private static String topic = "iron-kafka-test-" + randomUUID().toString();

    @BeforeClass
    public void setUp() throws Exception {
        // Start a new Kafka cluster
        m_kafkaCluster = KafkaCluster.createStarted(1);

        m_ironPath = createTempDirectory("iron-kafka-spi-");

        m_storeManager = initStoreManager();
    }

    private StoreManager initStoreManager() {
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, m_kafkaCluster.getConnectionString());
        return StoreManagerBuilder.newStoreManagerBuilder().
                withSnapshotSerializer(new JacksonSnapshotSerializerBuilder().get()).
                withSnapshotStore((new FileSnapshotStoreBuilder(topic)).setDir(m_ironPath).get()).
                withTransactionSerializer(new JacksonTransactionSerializerBuilder().get()).
                withTransactionStore((new KafkaTransactionStoreBuilder(topic)).setProperties(kafkaProperties).get()).
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
    public void tearDown() throws Exception {
        tryDeleteDirectory(m_ironPath);
        m_storeManager.close();
        m_kafkaCluster.close();
    }

    @Test
    public final void shouldOpenNewStore() {
        // When opening a new store
        Store store = m_storeManager.getStore("shouldOpenNewStore-" + randomUUID());
        // Then it's possible and there's nothing in it
        store.query(q -> {
            assertThat(q.select(Company.class).all()).isEmpty();
        });
    }

    @Test(enabled = false)
    public final void bench() throws Exception {
        String storeName = "shouldInsertCompanies" + randomUUID();
        int size = 200;

        List<Callable<Void>> tasks = IntStream.range(0, size).
                mapToObj(idx -> (Callable<Void>) () -> {
                    // Given a store
                    StoreManager manager = initStoreManager();
                    Store store = manager.getStore(storeName);

                    for (int i = 0; i < 100; i++) {
                        // When inserting 4 companies in one transaction
                        Store.TransactionBuilder transaction = store.begin();
                        transaction.addCommand(CreateCompany.class).set(CreateCompany::name).to("C" + idx + "." + i).set(CreateCompany::address).to("Palo Alto")
                                .submit();
                        transaction.submit().get();
                        System.out.println("Processed " + idx + "." + i);
                    }
                    System.out.println("Manager done : " + idx);
                    return null;
                }).
                collect(toList());

        ExecutorService executorService = Executors.newCachedThreadPool();
        try {
            invokeAll(executorService, tasks);
        } finally {
            executorService.shutdown();
        }
    }

    @Test
    public final void shouldInsertCompanies() throws Exception {
        // Given a store
        Store store = m_storeManager.getStore("shouldInsertCompanies" + randomUUID());
        // When inserting 4 companies in one transaction
        Store.TransactionBuilder transaction = store.begin();
        transaction.addCommand(CreateCompany.class).set(CreateCompany::name).to("Google").set(CreateCompany::address).to("Palo Alto").submit();
        transaction.addCommand(CreateCompany.class).set(CreateCompany::name).to("Microsoft").set(CreateCompany::address).to("Seattle").submit();
        transaction.addCommand(CreateCompany.class).set(CreateCompany::name).to("Axway").set(CreateCompany::address).to("Phoenix").submit();
        transaction.addCommand(CreateCompany.class).map(Map.of("name", "Apple", "address", "Cupertino")).submit();
        List<Object> result = transaction.submit().get();

        // Then they we are inserted and their identifiers are in ascending order
        assertThat(result).containsExactly(0L, 1L, 2L, 3L);

        // And we can retrieve them with queries
        store.query(q -> {
            Map<Long, Company> companies = q.select(Company.class).all().stream().collect(toMap(Company::id, identity()));
            assertThat(companies).hasSize(4);
            assertThat(companies.get(0L)).hasName("Google").hasAddress("Palo Alto");
            assertThat(companies.get(1L)).hasName("Microsoft").hasAddress("Seattle");
            assertThat(companies.get(2L)).hasName("Axway").hasAddress("Phoenix");
            assertThat(companies.get(3L)).hasName("Apple").hasAddress("Cupertino");
        });
    }

    @Test
    public final void shouldUpdateCompany() throws Exception {
        // Given a store
        Store store = m_storeManager.getStore("shouldUpdateCompany-" + randomUUID());
        // And a company in the database
        store.createCommand(CreateCompany.class).
                set(CreateCompany::name).to("Axway").
                set(CreateCompany::address).to("Phoenix").
                submit().
                get();

        // When updating it
        store.createCommand(ChangeCompanyAddress.class).
                set(ChangeCompanyAddress::name).to("Axway").
                set(ChangeCompanyAddress::newAddress).to("Puteaux").
                set(ChangeCompanyAddress::newCountry).to("France").
                submit().
                get();

        // Then it has the proper values
        store.query(q -> {
            Company axway = q.select(Company.class).where(Company::name).equalsTo("Axway");
            assertThat(axway).hasName("Axway").hasAddress("Puteaux").hasCountry("France");
        });
    }

    @Test
    public final void shouldDeleteCompany() throws Exception {
        // Given a store
        Store store = m_storeManager.getStore("shouldDeleteCompany-" + randomUUID());
        // And a company in the database
        store.createCommand(CreateCompany.class).
                set(CreateCompany::name).to("Axway").
                set(CreateCompany::address).to("Phoenix").
                submit().
                get();

        // When deleting it
        store.createCommand(DeleteCompany.class).
                set(DeleteCompany::name).to("Axway").
                submit().
                get();

        // Then it's deleted and the store is empty again
        store.query(q -> {
            assertThat(q.select(Company.class).all()).isEmpty();
        });
    }

    @Test
    public final void shouldResumeFromSnapshot() throws Exception {
        String storeName = "shouldResumeFromSnapshot" + randomUUID();

        // Given a store
        Store store = m_storeManager.getStore(storeName);
        // When inserting 4 companies in one transaction
        Store.TransactionBuilder transaction = store.begin();
        transaction.addCommand(CreateCompany.class).set(CreateCompany::name).to("Google").set(CreateCompany::address).to("Palo Alto").submit();
        transaction.addCommand(CreateCompany.class).set(CreateCompany::name).to("Microsoft").set(CreateCompany::address).to("Seattle").submit();
        transaction.addCommand(CreateCompany.class).set(CreateCompany::name).to("Axway").set(CreateCompany::address).to("Phoenix").submit();
        transaction.addCommand(CreateCompany.class).map(Map.of("name", "Apple", "address", "Cupertino")).submit();
        transaction.submit().get();

        // When creating a snapshot
        m_storeManager.snapshot();

        //close and reopen factory
        m_storeManager.close();
        m_storeManager = initStoreManager();

        // Then we can reopen the store and query the companies
        store = m_storeManager.getStore(storeName);
        store.query(q -> {
            Map<Long, Company> companies = q.select(Company.class).all().stream().collect(toMap(Company::id, identity()));
            assertThat(companies).hasSize(4);
            assertThat(companies.get(0L)).hasName("Google").hasAddress("Palo Alto");
            assertThat(companies.get(1L)).hasName("Microsoft").hasAddress("Seattle");
            assertThat(companies.get(2L)).hasName("Axway").hasAddress("Phoenix");
            assertThat(companies.get(3L)).hasName("Apple").hasAddress("Cupertino");
        });
    }

    @Test
    public void shouldStartFromInitialSnapshotWithNoTx() throws Exception {
        String storeName = "shouldStartFromInitialSnapshotWithNoTx" + randomUUID();

        // update topic to be sure that the tx ids restart at 0
        String saveTopic = topic;
        topic = "topic-" + storeName;

        m_storeManager.close();
        m_storeManager = initStoreManager();

        // Given a store
        Store store = m_storeManager.getStore(storeName);
        // When inserting 4 companies in one transaction
        Store.TransactionBuilder transaction = store.begin();
        transaction.addCommand(CreateCompany.class).set(CreateCompany::name).to("Google").set(CreateCompany::address).to("Palo Alto").submit();
        transaction.submit().get();

        // When creating a snapshot for id 0
        m_storeManager.snapshot();

        //close and reopen factory
        m_storeManager.close();

        // reset cluster (so empty and reset topic)
        m_kafkaCluster.close();
        m_kafkaCluster = KafkaCluster.createStarted(1);

        // init manager from snapshot at id 0 (equivalent to a bootstrap snapshot)
        m_storeManager = initStoreManager();
        // Then we can reopen the store and query the companies
        store = m_storeManager.getStore(storeName);
        store.query(q -> {
            Map<Long, Company> companies = q.select(Company.class).all().stream().collect(toMap(Company::id, identity()));
            assertThat(companies).hasSize(1);
            assertThat(companies.get(0L)).hasName("Google").hasAddress("Palo Alto");
        });

        // a new transaction would pass correctly
        transaction = store.begin();
        transaction.addCommand(CreateCompany.class).set(CreateCompany::name).to("Datadog").set(CreateCompany::address).to("Chatillon").submit();
        transaction.submit().get(2, SECONDS);

        store.query(q -> {
            Map<Long, Company> companies = q.select(Company.class).all().stream().collect(toMap(Company::id, identity()));
            assertThat(companies).hasSize(2);
            assertThat(companies.get(0L)).hasName("Google").hasAddress("Palo Alto");
            assertThat(companies.get(1L)).hasName("Datadog").hasAddress("Chatillon");
        });
        topic = saveTopic; //restore topic
    }
}
