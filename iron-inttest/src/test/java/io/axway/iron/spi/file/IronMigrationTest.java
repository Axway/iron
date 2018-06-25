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
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.of;
import static org.assertj.core.api.Java6Assertions.assertThat;

public class IronMigrationTest {

    @DataProvider(name = "ironCases")
    public Object[][] ironCases() {
        return new Object[][]{//
                {"simple iron", "iron", 2},//
                {"iron with many tenants", "ironFull", 9},//
        };
    }

    @Test(dataProvider = "ironCases")
    public void shouldMigrateIronSnapshotCorrectly(String message, String directory, int lastTenantIdx) throws IOException {
        Path randomPath = Paths.get("tmp-iron-test", "iron-spi-file-inttest", UUID.randomUUID().toString());
        try {
            Path sourceIronPath = randomPath.resolve(directory);
            Path destIronPath = randomPath.resolve(directory + ".new");
            // Given an Iron store
            HashMap<String, Set<String>> snapshotIdsByStoreType = copyIronFromResourcesToFileSytem(directory, lastTenantIdx, sourceIronPath);
            // When the Iron store is migrater
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
            assertThat(migratedIronPaths).as(message).containsExactlyInAnyOrderElementsOf(expectedPaths.collect(toList()));
        } finally {
            Files.walk(randomPath).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
    }

    @Nonnull
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
                                if (type.equals("snapshot")) {
                                    snapshotIdsByStoreType.computeIfAbsent(storeName.equals("global") ? "global" : "tenant", t -> new HashSet<>()).add(id);
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

    public List<String> getResourceFileAsString(String fileName) {
        InputStream is = getClass().getClassLoader().getResourceAsStream(fileName);
        if (is != null) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            return reader.lines().collect(toList());
        }
        return null;
    }
}
