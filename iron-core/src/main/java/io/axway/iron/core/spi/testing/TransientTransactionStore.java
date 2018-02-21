package io.axway.iron.core.spi.testing;

import java.io.*;
import java.math.BigInteger;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import io.axway.iron.error.StoreException;
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
                    throw new StoreException(e);
                }
            }
        };
    }

    @Override
    public void seekTransactionPoll(BigInteger latestProcessedTransactionId) {
        if (latestProcessedTransactionId.longValueExact() != 0) {
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
            throw new StoreException(e);
        }

        if (bytes != null) {
            long transactionId = m_consumerNextTxId.getAndIncrement();
            return new TransactionInput() {
                @Override
                public InputStream getInputStream() throws IOException {
                    return new ByteArrayInputStream(bytes);
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
