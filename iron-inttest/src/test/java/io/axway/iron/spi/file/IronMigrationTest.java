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
            Stream.concat(IntStream.range(0, lastTenantIdx + 1).boxed().map(n -> Integer.toString(n)), Stream.of("global")).forEach(storeName -> {
                for (String type : new String[]{"snapshot", "tx"}) {
                    String sourceDirectory = "io/axway/iron/spi/file/" + directory + "/" + storeName + "/" + type + "/";
                    String sourceFile = "00000000000000000000." + type;
                    List<String> fileContent = getResourceFileAsString(sourceDirectory + sourceFile);
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
                    throw new RuntimeException(exc);
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                    return FileVisitResult.CONTINUE;
                }
            });
            String destIronPathString = destIronPath.toString();
            String globalSnapshot = destIronPathString + "\\global\\snapshot\\00000000000000000000\\";
            String tenantSnapshot = destIronPathString + "\\tenant\\snapshot\\00000000000000000000\\";
            List<String> expectedFiles = Stream.concat(IntStream.range(0, lastTenantIdx + 1).boxed().map(n -> tenantSnapshot + n + ".snapshot"),
                                                       Stream.of(globalSnapshot + "global.snapshot")).collect(Collectors.toList());
            assertThat(foundFiles).as(message).containsExactlyInAnyOrderElementsOf(expectedFiles);
        } finally {
            Files.walk(randomPath).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
    }

    public List<String> getResourceFileAsString(String fileName) {
        InputStream is = getClass().getClassLoader().getResourceAsStream(fileName);
        if (is != null) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            return reader.lines().collect(Collectors.toList());
        }
        return null;
    }
}
