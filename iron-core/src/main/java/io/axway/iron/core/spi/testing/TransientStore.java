package io.axway.iron.core.spi.testing;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import org.reactivestreams.Publisher;
import com.google.common.collect.ImmutableList;
import io.axway.iron.error.StoreException;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;
import io.reactivex.processors.FlowableProcessor;
import io.reactivex.processors.PublishProcessor;

public class TransientStore implements SnapshotStore, TransactionStore {
    private final FlowableProcessor<TransactionInput> m_transactions = PublishProcessor.create();

    private long m_nextId;

    private final Object m_lock = new Object();

    TransientStore() {
    }

    @Override
    public OutputStream createSnapshotWriter(String storeName, BigInteger transactionId) {
        return new OutputStream() {
            @Override
            public void write(int b) {
                // discard all bytes
            }
        };
    }

    @Override
    public Publisher<StoreSnapshotReader> createSnapshotReader(BigInteger transactionId) {
        throw new StoreException("Snapshot has not been found", args -> args.add("snapshotTransactionId", transactionId));
    }

    @Override
    public List<BigInteger> listSnapshots() {
        return ImmutableList.of();
    }

    @Override
    public void close() {
        //nothing to do
    }

    @Override
    public void deleteSnapshot(BigInteger transactionId) {
        // do nothing
    }

    @Override
    public OutputStream createTransactionOutput(String storeName) {
        return new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();
                byte[] bytes = toByteArray();
                synchronized (m_lock) {
                    long id = m_nextId++;
                    m_transactions.onNext(new TransactionInput() {
                        @Override
                        public String storeName() {
                            return storeName;
                        }

                        @Override
                        public InputStream getInputStream() {
                            return new ByteArrayInputStream(bytes);
                        }

                        @Override
                        public BigInteger getTransactionId() {
                            return BigInteger.valueOf(id);
                        }
                    });
                }
            }
        };
    }

    @Override
    public Publisher<TransactionInput> allTransactions() {
        return m_transactions;
    }

    @Override
    public void seekTransaction(BigInteger latestProcessedTransactionId) {
        if (latestProcessedTransactionId.longValueExact() != 0) {
            throw new UnsupportedOperationException("Transient store doesn't support transaction store consumer seek");
        }
    }
}
