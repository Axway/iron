package io.axway.iron.core.spi.file;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.regex.*;
import java.util.stream.*;
import io.axway.iron.spi.storage.TransactionStore;

class FileTransactionStore implements TransactionStore {
    private static final String TX_EXT = "tx";
    private static final String FILENAME_FORMAT = "%020d.%s";
    private static final Pattern FILENAME_PATTERN = Pattern.compile("([0-9]{20}).([a-z]+)");

    private final Path m_transactionDir;
    private final Path m_transactionTmpDir;

    private final AtomicBigInteger m_tmpCounter = new AtomicBigInteger(BigInteger.ZERO);
    private final Object m_commitLock = new Object();
    private BigInteger m_nextTxId = BigInteger.ZERO;

    private BigInteger m_consumerNextTxId = BigInteger.ZERO;

    FileTransactionStore(Path transactionDir, Path transactionStoreTmpDir) {
        m_transactionDir = transactionDir;
        m_transactionTmpDir = transactionStoreTmpDir;

        m_nextTxId = retrieveNextTxId();
    }

    @Override
    public OutputStream createTransactionOutput() throws IOException {
        Path tmpFile = m_transactionTmpDir.resolve(getFileName(m_tmpCounter.getAndIncrement(), TX_EXT));

        return new BufferedOutputStream(Files.newOutputStream(tmpFile)) {
            @Override
            public void close() throws IOException {
                super.close();
                synchronized (m_commitLock) {
                    BigInteger transactionId = m_nextTxId;
                    m_nextTxId = m_nextTxId.add(BigInteger.ONE);
                    Path txFile = getTxFile(transactionId);

                    Files.move(tmpFile, txFile);
                    m_commitLock.notifyAll();
                }
            }
        };
    }

    @Override
    public void seekTransactionPoll(BigInteger latestProcessedTransactionId) {
        m_consumerNextTxId = latestProcessedTransactionId.add(BigInteger.ONE);
    }

    @Override
    public TransactionInput pollNextTransaction(long timeout, TimeUnit unit) {
        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
        BigInteger txId = m_consumerNextTxId;
        Path nextFile = getTxFile(txId);
        while (!Files.exists(nextFile)) {
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

        m_consumerNextTxId = m_consumerNextTxId.add(BigInteger.ONE);
        return new TransactionInput() {
            @Override
            public InputStream getInputStream() throws IOException {
                return new BufferedInputStream(Files.newInputStream(nextFile));
            }

            @Override
            public BigInteger getTransactionId() {
                return txId;
            }
        };
    }

    private BigInteger retrieveNextTxId() {
        try (Stream<Path> dirList = Files.list(m_transactionDir)) {
            return dirList //
                    .map(path -> path.getFileName().toString()) //
                    .map(FILENAME_PATTERN::matcher) //
                    .filter(matcher -> matcher.matches() && TX_EXT.equals(matcher.group(2))) //
                    .map(matcher -> new BigInteger(matcher.group(1))) //
                    .max(BigInteger::compareTo) //
                    .orElse(BigInteger.ONE.negate()) // in case no file exists we want nextTxId=0
                    .add(BigInteger.ONE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path getTxFile(BigInteger id) {
        return m_transactionDir.resolve(getFileName(id, TX_EXT));
    }

    private String getFileName(BigInteger id, String ext) {
        return String.format(FILENAME_FORMAT, id, ext);
    }
}
