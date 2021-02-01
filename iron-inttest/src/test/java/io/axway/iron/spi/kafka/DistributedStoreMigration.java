package io.axway.iron.spi.kafka;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import javax.annotation.*;
import org.assertj.core.api.Assertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import io.axway.iron.Command;
import io.axway.iron.Store;
import io.axway.iron.StoreManager;
import io.axway.iron.core.StoreManagerBuilder;
import io.axway.iron.core.spi.file.FileSnapshotStoreBuilder;
import io.axway.iron.sample.command.CreateSuperHeroV1;
import io.axway.iron.sample.command.CreateSuperHeroV2;
import io.axway.iron.sample.model.SuperHeroV1;
import io.axway.iron.sample.model.SuperHeroV2;
import io.axway.iron.spi.jackson.JacksonSnapshotSerializerBuilder;
import io.axway.iron.spi.jackson.JacksonTransactionSerializerBuilder;
import io.axway.iron.spi.model.snapshot.SerializableAttributeDefinition;
import io.axway.iron.spi.model.snapshot.SerializableSnapshot;

import static io.axway.iron.spi.kafka.Utils.tryDeleteDirectory;
import static java.nio.file.Files.createTempDirectory;
import static java.util.UUID.*;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

public class DistributedStoreMigration {
    private KafkaCluster m_kafkaCluster;
    private Path m_ironPath;

    private static String topic = "iron-kafka-test-" + randomUUID().toString();

    @BeforeClass
    public void setUp() throws Exception {
        // Start a new Kafka cluster
        m_kafkaCluster = KafkaCluster.createStarted(1);

        m_ironPath = createTempDirectory("iron-kafka-spi-");
    }

    private StoreManager initStoreManager(Class<?> entityClass, Class<? extends Command<?>> commandClass,
                                          @Nullable BiFunction<SerializableSnapshot, String, SerializableSnapshot> snapshotPostProcessor) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, m_kafkaCluster.getConnectionString());
        StoreManagerBuilder storeManagerBuilder = StoreManagerBuilder.newStoreManagerBuilder().
                withSnapshotSerializer(new JacksonSnapshotSerializerBuilder().get()).
                withSnapshotStore((new FileSnapshotStoreBuilder(topic)).setDir(m_ironPath).get()).
                withTransactionSerializer(new JacksonTransactionSerializerBuilder().get()).
                withTransactionStore((new KafkaTransactionStoreBuilder(topic)).setProperties(kafkaProperties).get()).
                withEntityClass(entityClass).
                withCommandClass(commandClass);
        Optional.ofNullable(snapshotPostProcessor).ifPresent(storeManagerBuilder::withSnapshotLoadingPostProcessor);
        return storeManagerBuilder.
                build();
    }

    @AfterClass
    public void tearDown() throws Exception {
        tryDeleteDirectory(m_ironPath);
        m_kafkaCluster.close();
    }

    @Test
    public final void shouldMigrateDistributedStore() throws ExecutionException, InterruptedException {
        StoreManager storeManagerNodeA = initStoreManager(SuperHeroV1.class, CreateSuperHeroV1.class, null);
        StoreManager storeManagerNodeB = initStoreManager(SuperHeroV1.class, CreateSuperHeroV1.class, null);

        Store kafkaStoreNodeA = storeManagerNodeA.getStore("kafkaStore");
        Store kafkaStoreNodeB = storeManagerNodeB.getStore("kafkaStore");

        // Creating two superheroes
        SuperHeroV1 bruceWayne = kafkaStoreNodeA.createCommand(CreateSuperHeroV1.class).
                set(CreateSuperHeroV1::firstName).to("Bruce").
                set(CreateSuperHeroV1::lastName).to("Wayne").
                submit().get();
        assertThat(bruceWayne.id()).isEqualTo(0);

        SuperHeroV1 bruceBanner = kafkaStoreNodeB.createCommand(CreateSuperHeroV1.class).
                set(CreateSuperHeroV1::firstName).to("Bruce").
                set(CreateSuperHeroV1::lastName).to("Banner").
                submit().get();
        assertThat(bruceBanner.id()).isEqualTo(1);

        // Waiting a bit so that the two stores are in sync
        Thread.sleep(500);

        // Checking creation is effective
        Collection<SuperHeroV1> contentFromStoreNodeA = kafkaStoreNodeA.query(readOnlyTransaction -> {
            return readOnlyTransaction.select(SuperHeroV1.class).all();
        });
        assertThat(contentFromStoreNodeA).hasSize(2);

        Collection<SuperHeroV1> contentFromStoreNodeB = kafkaStoreNodeB.query(readOnlyTransaction -> {
            return readOnlyTransaction.select(SuperHeroV1.class).all();
        });
        assertThat(contentFromStoreNodeB).hasSize(2);

        // Activating maintenance mode
        storeManagerNodeA.maintenance();

        // All store should be in maintenance
        Assertions.assertThatCode(() -> kafkaStoreNodeA.createCommand(CreateSuperHeroV1.class).
                set(CreateSuperHeroV1::firstName).to("Peter").
                set(CreateSuperHeroV1::lastName).to("Parker").
                submit().get()).
                hasMessageContaining("maintenance");
        Assertions.assertThatCode(() -> kafkaStoreNodeB.createCommand(CreateSuperHeroV1.class).
                set(CreateSuperHeroV1::firstName).to("Peter").
                set(CreateSuperHeroV1::lastName).to("Parker").
                submit().get()).
                hasMessageContaining("maintenance");

        // Restarting storeNodeA with new version
        storeManagerNodeA.snapshot();
        storeManagerNodeA.close();

        // Waiting for close to be effective
        Thread.sleep(500);

        StoreManager storeManagerNodeAMigrated = initStoreManager(SuperHeroV2.class, CreateSuperHeroV2.class, (snapshot, name) -> {
            System.out.println("migrating");
            if (snapshot.getApplicationModelVersion() >= 1) {
                return snapshot;
            }
            snapshot.setApplicationModelVersion(1);
            snapshot.getEntities().stream().filter(serializableEntity -> serializableEntity.getEntityName().equals("io.axway.iron.sample.model.SuperHeroV1"))
                    .findFirst().ifPresent(serializableEntity -> {
                serializableEntity.setEntityName("io.axway.iron.sample.model.SuperHeroV2");
                Map<String, SerializableAttributeDefinition> attributes = serializableEntity.getAttributes();
                attributes.put("nickName", attributes.get("firstName"));
                attributes.remove("firstName");
                attributes.remove("lastName");

                serializableEntity.getInstances().forEach(superHero -> {
                    Map<String, Object> values = superHero.getValues();
                    if (values.get("lastName").equals("Banner")) {
                        values.put("nickName", "Hulk");
                    }
                    if (values.get("lastName").equals("Wayne")) {
                        values.put("nickName", "Batman");
                    }
                    values.remove("firstName");
                    values.remove("lastName");
                });
            });
            return snapshot;
        });
        Store kafkaStoreNodeAMigrated = storeManagerNodeAMigrated.getStore("kafkaStore");
        Collection<SuperHeroV2> superHeroMigrated = kafkaStoreNodeAMigrated.query(readOnlyTransaction -> {
            return readOnlyTransaction.select(SuperHeroV2.class).all();
        });
        assertThat(superHeroMigrated.stream().map(SuperHeroV2::nickName)).containsExactlyInAnyOrder("Hulk", "Batman");

        System.out.println(superHeroMigrated);
    }
}
