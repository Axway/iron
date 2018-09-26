package io.axway.iron.spi.file;

import java.io.*;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.*;
import javax.annotation.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import com.google.common.base.Throwables;
import io.axway.iron.core.spi.file.IronMigration;
import io.axway.iron.error.StoreException;

import static java.lang.String.join;
import static java.util.Collections.*;
import static java.util.stream.Collectors.*;
import static java.util.stream.Stream.*;
import static org.assertj.core.api.Java6Assertions.assertThat;

public class IronMigrationTest {

    @DataProvider(name = "ironCases")
    public Object[][] ironCases() {
        return new Object[][]{//
                //{ message, directory, lastTenantIdx, @Nullable expectedErrorMessage },
                {"simple iron", "iron", 2, null},//
                {"ironWithRealData", "ironWithRealData", 1, null},//
                {"ironWithRealDataNoTransactionId", "ironWithRealDataNoTransactionId", 1, "transactionId not found once {\"args\": {\"found count\": 0}}"},//
                {"ironWithRealDataTwoTransactionId", "ironWithRealDataTwoTransactionId", 1, "transactionId not found once {\"args\": {\"found count\": 2}}"},//
        };
    }

    @Test(dataProvider = "ironCases")
    public void shouldMigrateIronSnapshotCorrectly(String message, String directory, int lastTenantIdx, @Nullable String expectedErrorMessage)
            throws IOException {
        Path randomPath = Paths.get("tmp-iron-test", "iron-spi-file-inttest", UUID.randomUUID().toString());
        try {
            Path sourceIronPath = randomPath.resolve(directory);
            Path destIronPath = randomPath.resolve(directory + ".new");
            // Given an Iron store
            HashMap<String, Set<String>> snapshotIdsByStoreType = copyIronFromResourcesToFileSystem(directory, lastTenantIdx, sourceIronPath);
            // When the Iron store is migrated
            IronMigration.main(new String[]{sourceIronPath.toString(), "global", "tenant", destIronPath.toString()});
            // Then migratedIronPaths contains the
            assertThat(expectedErrorMessage).as("Should have failed with error message: " + expectedErrorMessage).isNull();
            //
            List<Path> migratedIronPaths = new ArrayList<>();
            Files.walkFileTree(destIronPath, new FileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
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
            try (Stream<Path> expectedPaths = computeExpectedPaths(lastTenantIdx, destIronPath, snapshotIdsByStoreType)) {
                assertThat(migratedIronPaths).as(message).containsExactlyInAnyOrderElementsOf(expectedPaths.collect(toList()));
            }
        } catch (Exception e) {
            if (expectedErrorMessage != null && !expectedErrorMessage.equals(e.getMessage())) {
                throw e;
            }
        } finally {
            if (randomPath.toFile().exists()) {
                Files.walk(randomPath).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            }
        }
    }

    private Stream<Path> computeExpectedPaths(int lastTenantIdx, Path destIronPath, HashMap<String, Set<String>> snapshotIdsByStoreType) {
        List<Path> expectedPaths = new ArrayList<>();
        // Add global paths
        for (String usedSourceFileName : snapshotIdsByStoreType.getOrDefault("global", emptySet())) {
            Path globalSnapshot = destIronPath.resolve("global").resolve("snapshot").resolve(usedSourceFileName);
            expectedPaths.add(globalSnapshot.resolve("global.snapshot"));
        }
        // Add tenant paths
        for (String usedSourceFileName : snapshotIdsByStoreType.getOrDefault("tenant", emptySet())) {
            Path tenantSnapshot = destIronPath.resolve("tenant").resolve("snapshot").resolve(usedSourceFileName);
            IntStream.range(0, lastTenantIdx + 1).boxed().map(n -> tenantSnapshot.resolve(n + ".snapshot")).forEach(expectedPaths::add);
        }
        return expectedPaths.stream();
    }

    private HashMap<String, Set<String>> copyIronFromResourcesToFileSystem(String directory, int lastTenantIdx, Path sourceIronPath) {
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
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(fileName)) {
            if (is != null) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                return reader.lines().collect(toList());
            }
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
