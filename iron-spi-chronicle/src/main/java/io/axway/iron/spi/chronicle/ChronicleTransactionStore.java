package io.axway.iron.spi.chronicle;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.*;
import org.reactivestreams.Publisher;
import io.axway.iron.spi.StoreNamePrefixManagement;
import io.axway.iron.spi.storage.TransactionStore;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;

import static io.axway.iron.spi.StoreNamePrefixManagement.readStoreName;

public class ChronicleTransactionStore implements TransactionStore {
    private final ExcerptAppender m_appender;
    private final ExcerptTailer m_tailer;
    private final StoreNamePrefixManagement m_prefixManagement = new StoreNamePrefixManagement();
    private final Flowable<TransactionInput> m_transactionsFlow;
    private final ChronicleQueue m_chronicleQueue;

    ChronicleTransactionStore(Path chronicleStoreDir) {
        Path storeDir = ensureDirectoryExists(ensureDirectoryExists(chronicleStoreDir).resolve("tx"));
        m_chronicleQueue = SingleChronicleQueueBuilder.binary(storeDir).build();
        m_appender = m_chronicleQueue.acquireAppender();
        m_tailer = m_chronicleQueue.createTailer();

        m_transactionsFlow = Flowable.<TransactionInput>generate(emitter -> {
            AtomicReference<byte[]> bytesRef = new AtomicReference<>();
            boolean hasDocument = false;

            while (!hasDocument) {
                hasDocument = m_tailer.readDocument(wire -> bytesRef.set(wire.read().bytes()));
                if (!hasDocument) {
                    Thread.sleep(100);
                }
            }

            InputStream in = new ByteArrayInputStream(bytesRef.get());
            long transactionId = m_tailer.index();
            String store = readStoreName(in);
            emitter.onNext(new TransactionInput() {
                @Override
                public String storeName() {
                    return store;
                }

                @Override
                public InputStream getInputStream() {
                    return in;
                }

                @Override
                public BigInteger getTransactionId() {
                    return BigInteger.valueOf(transactionId);
                }
            });
        })                                   //
                .subscribeOn(Schedulers.io())        //
                .observeOn(Schedulers.computation());
    }

    @Override
    public OutputStream createTransactionOutput(String storeName) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();

                try (DocumentContext dc = m_appender.writingDocument()) {
                    dc.wire().write().bytes(toByteArray());
                }
            }
        };
        m_prefixManagement.writeNamePrefix(storeName, outputStream);
        return outputStream;
    }

    @Override
    public Publisher<TransactionInput> allTransactions() {
        return m_transactionsFlow;
    }

    @Override
    public void seekTransaction(BigInteger latestProcessedTransactionId) {
        m_tailer.moveToIndex(latestProcessedTransactionId.longValueExact());
    }

    @Override
    public void close() {
        m_chronicleQueue.close();
    }

    private Path ensureDirectoryExists(Path dir) {
        try {
            return Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
