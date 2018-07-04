package io.axway.iron.spi.file;

import java.io.*;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;
import javax.annotation.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import com.google.common.base.Throwables;
import io.axway.iron.StoreManager;
import io.axway.iron.core.StoreManagerBuilder;
import io.axway.iron.core.spi.file.IronMigration;
import io.axway.iron.error.StoreException;
import io.axway.iron.sample.command.CreatePerson;
import io.axway.iron.sample.model.Company;
import io.axway.iron.sample.model.Person;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

import static io.axway.iron.spi.file.FileTestHelper.buildFileSnapshotStore;
import static io.axway.iron.spi.file.FileTestHelper.buildFileTransactionStore;
import static io.axway.iron.spi.jackson.JacksonTestHelper.buildJacksonSnapshotSerializer;
import static io.axway.iron.spi.jackson.JacksonTestHelper.buildJacksonTransactionSerializer;
import static java.lang.String.join;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.of;
import static org.assertj.core.api.Java6Assertions.assertThat;

public class IronMigrationTest {

    private Function<Path, StoreManagerBuilder> personCompanyStoreManagerBuilder = destIronPath -> {
        Supplier<TransactionStore> transactionStoreFactory = () -> buildFileTransactionStore(destIronPath, "tenant");
        Supplier<SnapshotStore> snapshotStoreFactory = () -> buildFileSnapshotStore(destIronPath, "tenant");
        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
        return StoreManagerBuilder.newStoreManagerBuilder() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStore(transactionStoreFactory.get()) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStore(snapshotStoreFactory.get()) //
                .withEntityClass(Company.class) //
                .withEntityClass(Person.class) //
                .withCommandClass(CreatePerson.class);
    };

    @DataProvider(name = "ironCases")
    public Object[][] ironCases() {
        return new Object[][]{//
                {"simple iron", "iron", 2, null, null},//
                {"ironWithRealData", "ironWithRealData", 1, personCompanyStoreManagerBuilder, null},//
                {"ironWithRealDataNoTransactionId", "ironWithRealDataNoTransactionId", 1, personCompanyStoreManagerBuilder,
                        "Error occurred when recovering from latest snapshot"},//
                {"ironWithRealDataTwoTransactionId", "ironWithRealDataTwoTransactionId", 1, personCompanyStoreManagerBuilder,
                        "transactionId not found once {\"args\": {\"found count\": 2}}"},//
        };
    }

    @Test(dataProvider = "ironCases")
    public void shouldMigrateIronSnapshotCorrectly(String message, String directory, int lastTenantIdx,
                                                   @Nullable Function<Path, StoreManagerBuilder> storeManagerBuilder, @Nullable String expectedErrorMessage)
            throws IOException {
        Path randomPath = Paths.get("tmp-iron-test", "iron-spi-file-inttest", UUID.randomUUID().toString());
        try {
            Path sourceIronPath = randomPath.resolve(directory);
            Path destIronPath = randomPath.resolve(directory + ".new");
            // Given an Iron store
            HashMap<String, Set<String>> snapshotIdsByStoreType = copyIronFromResourcesToFileSytem(directory, lastTenantIdx, sourceIronPath);
            // When the Iron store is migrated
            IronMigration.main(new String[]{sourceIronPath.toString(), "global", "tenant", destIronPath.toString()});
            // Then migratedIronPaths contains the
            //
            List<Path> migratedIronPaths = new ArrayList<>();
            Files.walkFileTree(destIronPath, new FileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                    migratedIronPaths.add(path);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) {
                    throw new StoreException(exc);
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                    return FileVisitResult.CONTINUE;
                }
            });
            //
            Stream<Path> expectedPaths = Stream.empty();
            for (String usedSourceFileName : snapshotIdsByStoreType.getOrDefault("global", emptySet())) {
                Path globalSnapshot = destIronPath.resolve("global").resolve("snapshot").resolve(usedSourceFileName);
                expectedPaths = concat(expectedPaths, of(globalSnapshot.resolve("global.snapshot")));
            }
            for (String usedSourceFileName : snapshotIdsByStoreType.getOrDefault("tenant", emptySet())) {
                Path tenantSnapshot = destIronPath.resolve("tenant").resolve("snapshot").resolve(usedSourceFileName);
                expectedPaths = concat(expectedPaths, IntStream.range(0, lastTenantIdx + 1).boxed().map(n -> tenantSnapshot.resolve(n + ".snapshot")));
            }
            if (storeManagerBuilder != null) {
                StoreManagerBuilder factoryBuilder = storeManagerBuilder.apply(destIronPath);
                try (StoreManager ignored = factoryBuilder.build()) {
                }
            }
            assertThat(migratedIronPaths).as(message).containsExactlyInAnyOrderElementsOf(expectedPaths.collect(toList()));
        } catch (Exception e) {
            if (expectedErrorMessage != null && !expectedErrorMessage.equals(e.getMessage())) {
                throw e;
            }
        } finally {
            Files.walk(randomPath).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
    }

    private HashMap<String, Set<String>> copyIronFromResourcesToFileSytem(String directory, int lastTenantIdx, Path sourceIronPath) {
        HashMap<String, Set<String>> snapshotIdsByStoreType = new HashMap<>();
        concat(IntStream.range(0, lastTenantIdx + 1).boxed().map(n -> Integer.toString(n)), of("global")).
                forEach(storeName -> {
                    for (String type : new String[]{"snapshot", "tx"}) {
                        for (String id : new String[]{"00000000000000000000", "00000000000000000001", "00000000000000000002", "00000000000000000003"}) {
                            String sourceFile = id + "." + type;
                            List<String> fileContent = getResourceFileAsString(
                                    join("/", "io", "axway", "iron", "spi", "file", directory, storeName, type, sourceFile));
                            if (fileContent != null) {
                                if ("snapshot".equals(type)) {
                                    snapshotIdsByStoreType.computeIfAbsent("global".equals(storeName) ? "global" : "tenant", t -> new HashSet<>()).add(id);
                                }
                                Path destDirectoryPath = sourceIronPath.resolve(storeName).resolve(type);
                                destDirectoryPath.toFile().mkdirs();
                                Path destFilePath = destDirectoryPath.resolve(sourceFile);
                                try {
                                    Files.write(destFilePath, fileContent);
                                } catch (IOException e) {
                                    throw Throwables.propagate(e);
                                }
                            }
                        }
                    }
                });
        return snapshotIdsByStoreType;
    }

    private List<String> getResourceFileAsString(String fileName) {
        InputStream is = getClass().getClassLoader().getResourceAsStream(fileName);
        if (is != null) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            return reader.lines().collect(toList());
        }
        return null;
    }
}
