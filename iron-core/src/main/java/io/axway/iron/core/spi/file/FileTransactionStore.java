package io.axway.iron.core.spi.file;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.regex.*;
import io.axway.iron.spi.storage.TransactionStore;

class FileTransactionStore implements TransactionStore {
    private static final String TX_EXT = "tx";
    private static final String FILENAME_FORMAT = "%020d.%s";
    private static final Pattern FILENAME_PATTERN = Pattern.compile("([0-9]{20}).([a-z]+)");

    private final File m_transactionDir;
    private final File m_transactionTmpDir;

    private final AtomicLong m_tmpCounter = new AtomicLong();
    private final Object m_commitLock = new Object();
    private long m_nextTxId = 0;

    private long m_consumerNextTxId = 0;

    FileTransactionStore(File transactionDir) {
        m_transactionDir = Util.ensureDirectoryExists(transactionDir);
        m_transactionTmpDir = Util.ensureDirectoryExists(new File(m_transactionDir, ".tmp"));

        m_nextTxId = retrieveNextTxId();
    }

    @Override
    public OutputStream createTransactionOutput() throws IOException {
        File tmpFile = new File(m_transactionTmpDir, getFileName(m_tmpCounter.getAndIncrement(), TX_EXT));

        return new BufferedOutputStream(new FileOutputStream(tmpFile) {
            private volatile boolean m_closed = false;

            @Override
            public void close() throws IOException {
                super.close();
                synchronized (m_commitLock) {
                    // in case of multiple call to close() (eg due to FileOutputStream finalizer) the commit logic must not be performed again
                    if (!m_closed) {
                        m_closed = true;
                        long transactionId = m_nextTxId++;
                        File txFile = getTxFile(transactionId);
                        Util.rename(tmpFile, txFile);
                        m_commitLock.notifyAll();
                    }
                }
            }
        });
    }

    @Override
    public void seekTransactionPoll(long latestProcessedTransactionId) {
        m_consumerNextTxId = latestProcessedTransactionId + 1;
    }

    @Override
    public TransactionInput pollNextTransaction(long timeout, TimeUnit unit) {
        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
        long txId = m_consumerNextTxId;
        File nextFile = getTxFile(txId);
        while (!nextFile.exists()) {
            long waitTimeout = deadline - System.currentTimeMillis();
            if (waitTimeout > 0) {
                try {
                    synchronized (m_commitLock) {
                        m_commitLock.wait(waitTimeout);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            } else {
                return null;
            }
        }

        m_consumerNextTxId++;
        return new TransactionInput() {
            @Override
            public InputStream getInputStream() throws IOException {
                return new BufferedInputStream(new FileInputStream(nextFile));
            }

            @Override
            public long getTransactionId() {
                return txId;
            }
        };
    }

    private long retrieveNextTxId() {
        long nextTxId = 0;

        String[] list = m_transactionDir.list();
        if (list != null) {
            OptionalLong max = Arrays.stream(list) //
                    .map(FILENAME_PATTERN::matcher) //
                    .filter(matcher -> matcher.matches() && TX_EXT.equals(matcher.group(2))) //
                    .mapToLong(matcher -> Long.valueOf(matcher.group(1))) //
                    .max();
            if (max.isPresent()) {
                nextTxId = max.getAsLong() + 1;
            }
        }

        return nextTxId;
    }

    private File getTxFile(long id) {
        return new File(m_transactionDir, getFileName(id, TX_EXT));
    }

    private String getFileName(long id, String ext) {
        return String.format(FILENAME_FORMAT, id, ext);
    }
}
