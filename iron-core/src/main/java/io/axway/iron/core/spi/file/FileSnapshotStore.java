package io.axway.iron.core.spi.file;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.stream.*;
import io.axway.iron.spi.storage.SnapshotStore;

class FileSnapshotStore implements SnapshotStore {
    private static final String SNAPSHOT_EXT = "snapshot";
    private static final String FILENAME_FORMAT = "%020d.%s";
    private static final Pattern FILENAME_PATTERN = Pattern.compile("([0-9]{20}).([a-z]+)");

    private final File m_snapshotDir;
    private final File m_snapshotTmpDir;

    FileSnapshotStore(File snapshotDir) {
        m_snapshotDir = Util.ensureDirectoryExists(snapshotDir);
        m_snapshotTmpDir = Util.ensureDirectoryExists(new File(m_snapshotDir, ".tmp"));
    }

    @Override
    public OutputStream createSnapshotWriter(long transactionId) throws IOException {
        String snapshotFileName = getSnapshotFileName(transactionId);
        File tmpSnapshotFile = new File(m_snapshotTmpDir, snapshotFileName);
        File finalSnapshotFile = new File(m_snapshotDir, snapshotFileName);

        return new BufferedOutputStream(new FileOutputStream(tmpSnapshotFile) {
            private volatile boolean m_open = true;

            @Override
            public void close() throws IOException {
                super.close();
                // in case of multiple call to close() (eg due to FileOutputStream finalizer) the commit logic must not be performed again
                if (m_open) {
                    m_open = false;
                    Util.rename(tmpSnapshotFile, finalSnapshotFile);
                }
            }
        });
    }

    @Override
    public InputStream createSnapshotReader(long transactionId) throws IOException {
        File snapshotFile = new File(m_snapshotDir, getSnapshotFileName(transactionId));
        return new BufferedInputStream(new FileInputStream(snapshotFile));
    }

    @Override
    public List<Long> listSnapshots() {
        String[] list = m_snapshotDir.list();
        if (list == null) {
            return Collections.emptyList();
        }

        return Arrays.stream(list) //
                .map(FILENAME_PATTERN::matcher) //
                .filter(matcher -> matcher.matches() && SNAPSHOT_EXT.equals(matcher.group(2))) //
                .map(matcher -> Long.valueOf(matcher.group(1))) //
                .sorted() //
                .collect(Collectors.toList());
    }

    @Override
    public void deleteSnapshot(long transactionId) {
        File snapshotFile = new File(m_snapshotDir, getSnapshotFileName(transactionId));
        if (snapshotFile.exists() && !snapshotFile.delete()) {
            throw new UncheckedIOException(
                    new IOException("Cannot delete snapshot file '" + snapshotFile.getAbsolutePath() + "' for transaction id " + transactionId));
        }
        // ignore case when the snapshot to delete doesn't exist
    }

    private String getSnapshotFileName(long id) {
        return String.format(FILENAME_FORMAT, id, SNAPSHOT_EXT);
    }
}
