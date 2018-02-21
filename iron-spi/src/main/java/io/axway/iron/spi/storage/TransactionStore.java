package io.axway.iron.spi.storage;

import java.io.*;
import java.math.BigInteger;
import java.util.concurrent.*;

public interface TransactionStore {
    OutputStream createTransactionOutput() throws IOException;

    void seekTransactionPoll(BigInteger latestProcessedTransactionId);

    TransactionInput pollNextTransaction(long timeout, TimeUnit unit);

    interface TransactionInput {
        InputStream getInputStream() throws IOException;

        BigInteger getTransactionId();
    }
}
