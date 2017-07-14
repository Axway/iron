package io.axway.iron.core.spi.testing;

import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import io.axway.iron.spi.storage.TransactionStore;

class TransientTransactionStore implements TransactionStore {
    private final BlockingQueue<byte[]> m_transactions = new LinkedBlockingDeque<>();

    private final AtomicLong m_consumerNextTxId = new AtomicLong();

    @Override
    public OutputStream createTransactionOutput() throws IOException {
        return new ByteArrayOutputStream() {

            @Override
            public void close() throws IOException {
                super.close();
                byte[] bytes = toByteArray();
                try {
                    m_transactions.put(bytes);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    public void seekTransactionPoll(long latestProcessedTransactionId) {
        if (latestProcessedTransactionId != 0) {
            throw new UnsupportedOperationException("Transient store doesn't support transaction store consumer seek");
        }
    }

    @Override
    public TransactionInput pollNextTransaction(long timeout, TimeUnit unit) {
        byte[] bytes;
        try {
            bytes = m_transactions.poll(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        if (bytes != null) {
            long transactionId = m_consumerNextTxId.getAndIncrement();
            return new TransactionInput() {
                @Override
                public InputStream getInputStream() throws IOException {
                    return new ByteArrayInputStream(bytes);
                }

                @Override
                public long getTransactionId() {
                    return transactionId;
                }
            };
        } else {
            return null;
        }
    }
}
