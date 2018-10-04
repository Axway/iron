package io.axway.iron.core.spi.file;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.regex.*;
import java.util.stream.*;
import javax.annotation.*;
import javax.annotation.concurrent.*;
import org.reactivestreams.Publisher;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import io.axway.alf.log.Logger;
import io.axway.alf.log.LoggerFactory;
import io.axway.iron.error.StoreException;
import io.axway.iron.spi.storage.TransactionStore;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;

import static io.axway.iron.core.spi.file.FilenameUtils.*;

public class FileTransactionStore implements TransactionStore {
    private static final String TX_EXT = "tx";
    private static final Logger LOG = LoggerFactory.getLogger(FileTransactionStore.class);

    private final String m_filenameFormat;
    private final Pattern m_filenamePattern;
    private final Path m_transactionDir;
    private final Path m_transactionTmpDir;
    private long m_nextTxId;

    private final AtomicLong m_tmpCounter = new AtomicLong();
    private final Object m_commitLock = new Object();
    private final AtomicLong m_consumerStart = new AtomicLong();

    private final Lock m_txLock = new ReentrantLock();
    private final Condition m_txAvailable = m_txLock.newCondition();
    @GuardedBy("m_txLock")
    private final LinkedList<String> m_txToProcess = new LinkedList<>();

    private Publisher<TransactionInput> m_allTx;

    FileTransactionStore(Path fileStoreDir, @Nullable Integer transactionIdLength) {
        m_filenamePattern = Pattern.compile("(" + buildIdRegex(transactionIdLength) + ")_([\\w\\s-]+)\\.([a-zA-Z]+)");
        m_filenameFormat = buildIdFormat(transactionIdLength) + "_%s.%s";
        m_transactionDir = ensureDirectoryExists(fileStoreDir.resolve("tx"));
        m_transactionTmpDir = ensureDirectoryExists(fileStoreDir.resolve(".tmp").resolve("tx"));
        m_nextTxId = retrieveNextTxId();
        initExistingFiles();
    }

    @Override
    public OutputStream createTransactionOutput(String storeName) throws IOException {
        Path tmpFile = m_transactionTmpDir.resolve(getFileName(m_tmpCounter.getAndIncrement(), storeName));

        return new BufferedOutputStream(Files.newOutputStream(tmpFile)) {
            @Override
            public void close() throws IOException {
                super.close();
                synchronized (m_commitLock) {
                    long transactionId = m_nextTxId++;
                    String fileName = getFileName(transactionId, storeName);
                    Path txFile = m_transactionDir.resolve(fileName);

                    Files.move(tmpFile, txFile);
                    m_txLock.lock();
                    try {
                        m_txToProcess.add(fileName);
                        m_txAvailable.signalAll();
                    } finally {
                        m_txLock.unlock();
                    }
                }
            }
        };
    }

    private String getFileName(long id, String storeName) {
        return String.format(m_filenameFormat, id, storeName, TX_EXT);
    }

    @Override
    public Publisher<TransactionInput> allTransactions() {
        if (m_allTx == null) {
            m_allTx = Flowable                //
                    .<String>generate(emitter -> {
                        String next;
                        m_txLock.lock();
                        try {
                            while (m_txToProcess.isEmpty()) {
                                m_txAvailable.await();
                            }
                            next = m_txToProcess.poll();
                        } finally {
                            m_txLock.unlock();
                        }
                        emitter.onNext(next);
                    })//
                    .subscribeOn(Schedulers.io())                   //
                    .observeOn(Schedulers.computation())            //
                    .map(fileName -> {
                        Matcher matcher = m_filenamePattern.matcher(fileName);
                        if (!matcher.matches()) {
                            throw new StoreException("Transaction file name in transaction directory does not match the expected pattern",
                                                     args -> args.add("filename", fileName).add("transactionDirectory", m_transactionDir)
                                                             .add("fileNamePattern", m_filenamePattern.toString()));
                        }
                        String store = matcher.group(2);
                        BigInteger id = BigInteger.valueOf(Long.valueOf(matcher.group(1)));
                        return new TransactionInput() {
                            @Override
                            public String storeName() {
                                return store;
                            }

                            @Override
                            public InputStream getInputStream() throws IOException {
                                return new BufferedInputStream(Files.newInputStream(m_transactionDir.resolve(fileName)));
                            }

                            @Override
                            public BigInteger getTransactionId() {
                                return id;
                            }
                        };
                    });
        }
        return m_allTx;
    }

    private void initExistingFiles() {
        m_txLock.lock();
        try {
            m_txToProcess.clear();
            try (Stream<Path> pathStream = Files.find(m_transactionDir, 1, (path, atts) -> {
                if (atts.isDirectory()) {
                    return false;
                }
                String fileName = path.getFileName().toString();
                return fileName.compareTo(Strings.padStart(m_consumerStart.toString(), 20, '0')) >= 0;
            })) {
                pathStream.forEach(path -> {
                    String fileName = path.getFileName().toString();
                    if (m_filenamePattern.matcher(fileName).matches()) {
                        m_txToProcess.add(fileName);
                        m_txAvailable.signalAll();
                    }
                });
            }
            Collections.sort(m_txToProcess);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        } finally {
            m_txLock.unlock();
        }
    }

    @Override
    public void seekTransaction(BigInteger latestProcessedTransactionId) {
        if (latestProcessedTransactionId.longValueExact() >= m_nextTxId) {
            LOG.warn("The next transaction id has been set to the transaction id of the last snapshot because the first was lower than the second.",
                     args -> args.add("next transaction id", m_nextTxId).add("transaction id of the last snapshot", latestProcessedTransactionId));
            m_nextTxId = latestProcessedTransactionId.add(BigInteger.ONE).longValueExact();
        }
        m_consumerStart.set(latestProcessedTransactionId.longValueExact() + 1);
        m_allTx = null;
        initExistingFiles();
    }

    private long retrieveNextTxId() {
        try (Stream<Path> dirList = Files.list(m_transactionDir)) {
            return dirList //
                    .map(path -> path.getFileName().toString()) //
                    .map(m_filenamePattern::matcher) //
                    .filter(matcher -> matcher.matches() && TX_EXT.equals(matcher.group(3))) //
                    .mapToLong(matcher -> Long.valueOf(matcher.group(1))) //
                    .max() //
                    .orElse(-1L) // in case no file exists we want nextTxId=0
                    + 1;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
