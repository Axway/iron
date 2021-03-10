package io.axway.iron.spi.file;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import javax.annotation.*;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import io.axway.iron.Command;
import io.axway.iron.Store;
import io.axway.iron.StoreManager;
import io.axway.iron.core.StoreManagerBuilder;
import io.axway.iron.core.spi.file.FileSnapshotStoreBuilder;
import io.axway.iron.core.spi.file.FileTransactionStoreBuilder;
import io.axway.iron.error.ReadonlyException;
import io.axway.iron.sample.command.CreateSuperHeroV1;
import io.axway.iron.sample.command.CreateSuperHeroV2;
import io.axway.iron.sample.model.SuperHeroV1;
import io.axway.iron.sample.model.SuperHeroV2;
import io.axway.iron.spi.jackson.JacksonSnapshotSerializerBuilder;
import io.axway.iron.spi.jackson.JacksonTransactionSerializerBuilder;
import io.axway.iron.spi.model.snapshot.SerializableAttributeDefinition;
import io.axway.iron.spi.model.snapshot.SerializableSnapshot;

import static io.axway.iron.spi.Utils.tryDeleteDirectory;
import static java.nio.file.Files.createTempDirectory;
import static java.util.UUID.*;
import static org.assertj.core.api.Assertions.*;

public class BlueGreenMigrationTest {
    private static final String storeName = "iron-bluegreen-test-" + randomUUID().toString();
    public static final String STORE_NAME = "myStore";
    private Path m_snapshotDirNodeBlue;
    private Path m_txDirNodeBlue;
    private Path m_txDirNodeGreen;

    @BeforeClass
    public void setUp() throws Exception {
        m_snapshotDirNodeBlue = createTempDirectory("snapshotBlue-");
        m_txDirNodeBlue = createTempDirectory("txBlue-");
        m_txDirNodeGreen = createTempDirectory("txGreen-");
    }

    @AfterClass
    public void tearDown() {
        tryDeleteDirectory(m_snapshotDirNodeBlue);
        tryDeleteDirectory(m_txDirNodeBlue);
        tryDeleteDirectory(m_txDirNodeGreen);
    }

    @Test
    public final void shouldMigrateDistributedStore() throws ExecutionException, InterruptedException {

        try (StoreManager storeManagerNodeBlue = initStoreManager(m_snapshotDirNodeBlue, m_txDirNodeBlue, SuperHeroV1.class, CreateSuperHeroV1.class, null)) {

            Store myStoreNodeBlue = storeManagerNodeBlue.getStore(STORE_NAME);

            // Creating two superheroes
            SuperHeroV1 bruceWayne = myStoreNodeBlue.createCommand(CreateSuperHeroV1.class).
                    set(CreateSuperHeroV1::firstName).to("Bruce").
                    set(CreateSuperHeroV1::lastName).to("Wayne").
                    submit().get();
            assertThat(bruceWayne.id()).isEqualTo(0);

            // Checking creation is effective
            Collection<SuperHeroV1> contentFromStoreNodeBlue = myStoreNodeBlue.query(readonlyTransaction -> {
                return readonlyTransaction.select(SuperHeroV1.class).all();
            });
            assertThat(contentFromStoreNodeBlue).hasSize(1);

            // Activating maintenance mode
            storeManagerNodeBlue.setReadonly(true);

            // All store should be in maintenance
            assertThatCode(() -> myStoreNodeBlue.createCommand(CreateSuperHeroV1.class).
                    set(CreateSuperHeroV1::firstName).to("Peter").
                    set(CreateSuperHeroV1::lastName).to("Parker").
                    submit().get()).
                    withFailMessage("A write command shouldn't be processed by a readonly store").
                    hasRootCauseInstanceOf(ReadonlyException.class);
        }

        // Reopening storeManagerNodeBlue should be in readonly
        try (StoreManager storeManagerNodeBlue = initStoreManager(m_snapshotDirNodeBlue, m_txDirNodeBlue, SuperHeroV1.class, CreateSuperHeroV1.class, null)) {
            assertThat(storeManagerNodeBlue.isReadonly()).
                    withFailMessage("Restarting a readonly store should preserve its state provided you point the same tx store").
                    isTrue();

            Store myStoreNodeBlue = storeManagerNodeBlue.getStore(STORE_NAME);
            assertThatCode(() -> myStoreNodeBlue.createCommand(CreateSuperHeroV1.class).
                    set(CreateSuperHeroV1::firstName).to("Peter").
                    set(CreateSuperHeroV1::lastName).to("Parker").
                    submit().get()).
                    withFailMessage("A write command shouldn't be processed by a readonly store").
                    hasRootCauseInstanceOf(ReadonlyException.class);
        }

        // Definition of the migration from superHeroV1 to V2
        BiFunction<SerializableSnapshot, String, SerializableSnapshot> migration = buildMigrationFromSuperHeroV1ToV2();

        // Initiating a new migrated store manager with the same content for snapshot but brand new tx dir
        try (StoreManager storeManagerNodeGreen = initStoreManager(m_snapshotDirNodeBlue, m_txDirNodeGreen, SuperHeroV2.class, CreateSuperHeroV2.class,
                                                                   migration)) {
            assertThat(storeManagerNodeGreen.isReadonly()).
                    withFailMessage("ReadLock is on tx store so starting this with its proper tx store shouldn't resume it as readonly ").
                    isFalse();

            // Checking content as a SuperHeroV2
            Store myStoreNodeAMigrated = storeManagerNodeGreen.getStore(STORE_NAME);
            Collection<SuperHeroV2> superHeroMigrated = myStoreNodeAMigrated.query(readonlyTransaction -> {
                return readonlyTransaction.select(SuperHeroV2.class).all();
            });
            assertThat(superHeroMigrated.stream().map(SuperHeroV2::nickName)).
                    withFailMessage("Once migrated, the super hero should be none by its nickname").
                    containsExactlyInAnyOrder("Batman");

            // Checking that store is not anymore in readonly
            assertThatCode(() -> myStoreNodeAMigrated.createCommand(CreateSuperHeroV2.class).
                    set(CreateSuperHeroV2::nickName).to("Spiderman").
                    submit().get()).doesNotThrowAnyException();

            // Checking that mutation is effective
            String newSuperHeroName = myStoreNodeAMigrated.query(readonlyTransaction -> {
                return readonlyTransaction.select(SuperHeroV2.class).all().
                        stream().
                        filter(superHeroV2 -> superHeroV2.nickName().equals("Spiderman")).
                        findFirst().
                        map(SuperHeroV2::nickName).
                        orElseThrow();
            });
            assertThat(newSuperHeroName).isEqualTo("Spiderman");
            System.out.println("Here comes : " + superHeroMigrated);
        }
    }

    @NotNull
    private BiFunction<SerializableSnapshot, String, SerializableSnapshot> buildMigrationFromSuperHeroV1ToV2() {
        return (snapshot, name) -> {
            if (snapshot.getApplicationModelVersion() >= 1) {
                return snapshot;
            }
            snapshot.setApplicationModelVersion(1);
            snapshot.getEntities().stream().
                    filter(serializableEntity -> serializableEntity.getEntityName().equals(SuperHeroV1.class.getName())).
                    findFirst().
                    ifPresent(serializableEntity -> {
                        serializableEntity.setEntityName(SuperHeroV2.class.getName());
                        Map<String, SerializableAttributeDefinition> attributes = serializableEntity.getAttributes();
                        attributes.put("nickName", attributes.get("firstName"));
                        attributes.remove("firstName");
                        attributes.remove("lastName");

                        serializableEntity.getInstances().
                                forEach(superHero -> {
                                    Map<String, Object> values = superHero.getValues();
                                    if (values.get("lastName").equals("Wayne")) {
                                        values.put("nickName", "Batman");
                                    }
                                    values.remove("firstName");
                                    values.remove("lastName");
                                });
                    });
            return snapshot;
        };
    }

    private StoreManager initStoreManager(Path snapshotWorkingDir, Path txWorkingDir, Class<?> entityClass, Class<? extends Command<?>> commandClass,
                                          @Nullable BiFunction<SerializableSnapshot, String, SerializableSnapshot> snapshotPostProcessor) {
        StoreManagerBuilder storeManagerBuilder = StoreManagerBuilder.newStoreManagerBuilder().
                withSnapshotSerializer(new JacksonSnapshotSerializerBuilder().get()).
                withSnapshotStore((new FileSnapshotStoreBuilder(storeName)).setDir(snapshotWorkingDir).get()).
                withTransactionSerializer(new JacksonTransactionSerializerBuilder().get()).
                withTransactionStore((new FileTransactionStoreBuilder(storeName)).setDir(txWorkingDir).get()).
                withEntityClass(entityClass).
                withCommandClass(commandClass);
        Optional.ofNullable(snapshotPostProcessor).ifPresent(storeManagerBuilder::withSnapshotLoadingPostProcessor);
        return storeManagerBuilder.
                build();
    }
}
