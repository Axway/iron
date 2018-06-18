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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import com.google.common.base.Throwables;
import io.axway.iron.core.spi.file.IronMigration;
import io.axway.iron.error.StoreException;

import static java.lang.String.join;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.of;
import static org.assertj.core.api.Java6Assertions.assertThat;

public class IronMigrationTest {

    @DataProvider(name = "ironCases")
    public Object[][] ironCases() {
        return new Object[][]{//
                {"simple iron", "iron", 2},//
                {"full iron", "ironFull", 9}//
        };
    }

    @Test(dataProvider = "ironCases")
    public void test(String message, String directory, int lastTenantIdx) throws IOException {
        Path randomPath = Paths.get("tmp-iron-test", "iron-spi-file-inttest", UUID.randomUUID().toString());
        try {
            Path sourceIronPath = randomPath.resolve(directory);
            Path destIronPath = randomPath.resolve(directory + ".new");
            concat(IntStream.range(0, lastTenantIdx + 1).boxed().map(n -> Integer.toString(n)), of("global")).forEach(storeName -> {
                for (String type : new String[]{"snapshot", "tx"}) {
                    String sourceFile = "00000000000000000000." + type;
                    List<String> fileContent = getResourceFileAsString(join("/", "io", "axway", "iron", "spi", "file", directory, storeName, type, sourceFile));
                    Path destDirectoryPath = sourceIronPath.resolve(storeName).resolve(type);
                    destDirectoryPath.toFile().mkdirs();
                    Path destFilePath = destDirectoryPath.resolve(sourceFile);
                    try {
                        Files.write(destFilePath, fileContent);
                    } catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }
            });
            IronMigration.main(new String[]{sourceIronPath.toString(), "global", "tenant", destIronPath.toString()});
            Set<String> foundFiles = new HashSet<>();
            Files.walkFileTree(destIronPath, new FileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    foundFiles.add(file.toString());
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
            Path globalSnapshot = destIronPath.resolve("global").resolve("snapshot").resolve("00000000000000000000");
            Path tenantSnapshot = destIronPath.resolve("tenant").resolve("snapshot").resolve("00000000000000000000");
            List<Path> expectedPaths = concat(IntStream.range(0, lastTenantIdx + 1).boxed().map(n -> tenantSnapshot.resolve(n + ".snapshot")),
                                              of(globalSnapshot.resolve("global.snapshot"))).collect(toList());
            assertThat(foundFiles.stream().map(Paths::get).collect(toList())).as(message).containsExactlyInAnyOrderElementsOf(expectedPaths);
        } finally {
            Files.walk(randomPath).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
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
