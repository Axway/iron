package io.axway.iron.core.spi.file;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.*;
import java.util.stream.*;
import javax.annotation.*;
import io.axway.iron.spi.storage.SnapshotStore;

class FileSnapshotStore implements SnapshotStore {
    private static final String SNAPSHOT_EXT = "snapshot";
    private final String m_filenameFormat;
    private final Pattern m_filenamePattern;

    private final Path m_snapshotDir;
    private final Path m_snapshotTmpDir;

    FileSnapshotStore(Path snapshotDir, Path snapshotStoreTmpDir, @Nullable Integer limitedSize) {
        m_snapshotDir = snapshotDir;
        m_snapshotTmpDir = snapshotStoreTmpDir;
        m_filenamePattern = FilenameUtils.buildFilenamePattern(limitedSize);
        m_filenameFormat = FilenameUtils.buildFilenameFormat(limitedSize);
    }

    @Override
    public OutputStream createSnapshotWriter(BigInteger transactionId) throws IOException {
        String snapshotFileName = getSnapshotFileName(transactionId);
        Path tmpSnapshotFile = m_snapshotTmpDir.resolve(snapshotFileName);
        Path finalSnapshotFile = m_snapshotDir.resolve(snapshotFileName);

        return new BufferedOutputStream(Files.newOutputStream(tmpSnapshotFile)) {
            @Override
            public void close() throws IOException {
                super.close();
                Files.move(tmpSnapshotFile, finalSnapshotFile);
            }
        };
    }

    @Override
    public InputStream createSnapshotReader(BigInteger transactionId) throws IOException {
        Path snapshotFile = m_snapshotDir.resolve(getSnapshotFileName(transactionId));
        return new BufferedInputStream(Files.newInputStream(snapshotFile));
    }

    @Override
    public List<BigInteger> listSnapshots() {
        try (Stream<Path> dirList = Files.list(m_snapshotDir)) {
            return dirList //
                    .map(path -> path.getFileName().toString()) //
                    .map(m_filenamePattern::matcher) //
                    .filter(matcher -> matcher.matches() && SNAPSHOT_EXT.equals(matcher.group(2))) //
                    .map(matcher -> new BigInteger(matcher.group(1))).sorted() //
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void deleteSnapshot(BigInteger transactionId) {
        Path snapshotFile = m_snapshotDir.resolve(getSnapshotFileName(transactionId));
        if (Files.exists(snapshotFile)) {
            try {
                Files.delete(snapshotFile);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        // ignore case when the snapshot to delete doesn't exist
    }

    private String getSnapshotFileName(BigInteger id) {
        return String.format(m_filenameFormat, id, SNAPSHOT_EXT);
    }
}
