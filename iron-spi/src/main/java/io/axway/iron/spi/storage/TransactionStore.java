package io.axway.iron.spi.storage;

import java.io.*;
import java.util.concurrent.*;

public interface TransactionStore {
    OutputStream createTransactionOutput() throws IOException;

    void seekTransactionPoll(long latestProcessedTransactionId);

    TransactionInput pollNextTransaction(long timeout, TimeUnit unit);

    interface TransactionInput {
        InputStream getInputStream() throws IOException;

        long getTransactionId();
    }
}
