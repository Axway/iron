package io.axway.iron.spi.file;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import io.axway.iron.core.spi.file.FileSnapshotStoreBuilder;
import io.axway.iron.core.spi.file.FileTransactionStoreBuilder;
import io.axway.iron.spi.aws.LayoutMigrationV2ToV3IT;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

import static java.lang.String.join;
import static java.util.stream.Collectors.*;

public class FileTestHelper {

    public static SnapshotStore buildFileSnapshotStore(Path filePath, String name) {
        return new FileSnapshotStoreBuilder(name).setDir(filePath).get();
    }

    public static TransactionStore buildFileTransactionStore(Path filePath, String name) {
        return new FileTransactionStoreBuilder(name).setDir(filePath).get();
    }

    public static SnapshotStore buildFileSnapshotStore(Path filePath, String name, Integer transactionIdPaddingLength) {
        return new FileSnapshotStoreBuilder(name).setDir(filePath).setTransactionIdLength(transactionIdPaddingLength).get();
    }

    public static TransactionStore buildFileTransactionStore(Path filePath, String name, Integer transactionIdPaddingLength) {
        return new FileTransactionStoreBuilder(name).setDir(filePath).setTransactionIdPaddingLength(transactionIdPaddingLength).get();
    }

    public static List<String> getResourceFileAsString(Class clazz, String fileName) {
        try (InputStream is = clazz.getClassLoader().getResourceAsStream(fileName)) {
            if (is != null) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                return reader.lines().collect(toList());
            }
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void copyResource(Path dest, String... elements) throws IOException {
        String resource = join("/", elements);
        Path destPath = Paths.get(dest.toString(), elements);
        Files.createDirectories(destPath.getParent());
        List<String> content = getResourceFileAsString(LayoutMigrationV2ToV3IT.class, resource);
        Files.write(destPath, content);
    }
}
