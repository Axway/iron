package io.axway.iron.spi.chronicle;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import io.axway.iron.spi.storage.TransactionStore;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;

class ChronicleTransactionStore implements TransactionStore {

    private final ExcerptAppender m_appender;
    private final ExcerptTailer m_tailer;

    ChronicleTransactionStore(Path transactionDir) {
        ChronicleQueue chronicleQueue = SingleChronicleQueueBuilder.binary(transactionDir).build();
        m_appender = chronicleQueue.acquireAppender();
        m_tailer = chronicleQueue.createTailer();
    }

    @Override
    public OutputStream createTransactionOutput() throws IOException {
        return new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();

                try (DocumentContext dc = m_appender.writingDocument()) {
                    dc.wire().write().bytes(toByteArray());
                }
            }
        };
    }

    @Override
    public void seekTransactionPoll(BigInteger latestProcessedTransactionId) {
        m_tailer.moveToIndex(latestProcessedTransactionId.longValueExact());
    }

    @Override
    public TransactionInput pollNextTransaction(long timeout, TimeUnit unit) {
        AtomicReference<byte[]> bytesRef = new AtomicReference<>();
        boolean hasDocument = m_tailer.readDocument(wire -> bytesRef.set(wire.read().bytes()));

        if (hasDocument) {
            InputStream in = new ByteArrayInputStream(bytesRef.get());
            long transactionId = m_tailer.index();
            return new TransactionInput() {
                @Override
                public InputStream getInputStream() throws IOException {
                    return in;
                }

                @Override
                public BigInteger getTransactionId() {
                    return BigInteger.valueOf(transactionId);
                }
            };
        } else {
            return null;
        }
    }
}
