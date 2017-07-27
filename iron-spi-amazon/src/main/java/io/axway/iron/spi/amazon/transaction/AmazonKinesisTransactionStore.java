package io.axway.iron.spi.amazon.transaction;

import java.io.*;
import java.util.concurrent.*;
import io.axway.iron.spi.storage.TransactionStore;

public class AmazonKinesisTransactionStore implements TransactionStore {
    @Override
    public OutputStream createTransactionOutput() throws IOException {
        return null;  // TODO implement method
    }

    @Override
    public void seekTransactionPoll(long latestProcessedTransactionId) {
        // TODO implement method
    }

    @Override
    public TransactionInput pollNextTransaction(long timeout, TimeUnit unit) {
        return null;  // TODO implement method
    }
}
