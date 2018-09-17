package io.axway.iron.core.spi.file;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.*;
import java.util.stream.*;
import javax.annotation.*;
import org.reactivestreams.Publisher;
import io.axway.iron.spi.storage.SnapshotStore;
import io.reactivex.Flowable;

import static io.axway.iron.core.spi.file.FilenameUtils.*;
import static java.lang.String.format;
import static java.nio.file.Files.*;

public class FileSnapshotStore implements SnapshotStore {
    private static final String SNAPSHOT_EXT = "snapshot";

    private final String m_filenameFormat;
    private final Pattern m_filenamePattern;
    private final String m_idFormat;
    private final String m_idRegex;
    private final Path m_snapshotTmpDir;
    private final Path m_snapshotDir;

    FileSnapshotStore(Path snapshotDir, @Nullable Integer transactionIdLength) {
        m_filenamePattern = Pattern.compile("([\\w\\s-]+)\\.([a-zA-Z]+)");
        m_filenameFormat = "%s.%s";
        m_idFormat = buildIdFormat(transactionIdLength);
        m_idRegex = buildIdRegex(transactionIdLength);
        m_snapshotDir = ensureDirectoryExists(snapshotDir.resolve("snapshot"));
        m_snapshotTmpDir = ensureDirectoryExists(snapshotDir.resolve(".tmp").resolve("snapshot"));
    }

    @Override
    public OutputStream createSnapshotWriter(String storeName, BigInteger transactionId) throws IOException {
        String snapshotFileName = format(m_filenameFormat, storeName, SNAPSHOT_EXT);
        Path tmpSnapshotFile = ensureDirectoryExists(m_snapshotTmpDir.resolve(format(m_idFormat, transactionId))).resolve(snapshotFileName);
        Path finalSnapshotFile = getSnapshotDirectory(transactionId).resolve(snapshotFileName);
        return new BufferedOutputStream(newOutputStream(tmpSnapshotFile)) {
            @Override
            public void close() throws IOException {
                super.close();
                move(tmpSnapshotFile, finalSnapshotFile);
            }
        };
    }

    @Override
    public Publisher<StoreSnapshotReader> createSnapshotReader(BigInteger transactionId) {
        //noinspection ConstantConditions
        return Flowable                                                                  //
                .fromArray(getSnapshotDirectory(transactionId).toFile().listFiles())     //                       //
                .flatMap(file -> {
                    Matcher matcher = m_filenamePattern.matcher(file.getName());
                    if (matcher.matches() && matcher.group(2).equals(SNAPSHOT_EXT)) {
                        String store = matcher.group(1);
                        return Flowable.just(new StoreSnapshotReader() {
                            @Override
                            public String storeName() {
                                return store;
                            }

                            @Override
                            public InputStream inputStream() throws FileNotFoundException {
                                return new BufferedInputStream(new FileInputStream(file));
                            }
                        });
                    } else {
                        return Flowable.empty();
                    }
                });
    }

    @Override
    public List<BigInteger> listSnapshots() throws IOException {
        try (Stream<Path> list = list(m_snapshotDir)) {
            return list                                           //
                    .map(path -> path.getFileName().toString())   //
                    .filter(name -> name.matches(m_idRegex))      //
                    .map(BigInteger::new)                         //
                    .collect(Collectors.toList());
        }
    }

    @Override
    public void deleteSnapshot(BigInteger transactionId) throws IOException {
        try (Stream<Path> walkStream = walk(getSnapshotDirectory(transactionId))) {
            walkStream        //
                    .sorted(Comparator.reverseOrder())       //
                    .map(Path::toFile)                       //
                    .forEach(File::delete);
        }
    }

    @Nonnull
    private Path getSnapshotDirectory(BigInteger transactionId) {
        return ensureDirectoryExists(m_snapshotDir.resolve(format(m_idFormat, transactionId)));
    }
}
