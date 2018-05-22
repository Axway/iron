package io.axway.iron.spi.storage;

import java.io.*;
import java.math.BigInteger;
import org.reactivestreams.Publisher;

/**
 * SPI for transaction store.
 */
public interface TransactionStore {
    /**
     * Retrieves an {@link OutputStream} to write the content of a new transaction for the given store
     * @param storeName the name of the store in which the transaction is performed
     * @throws IOException in case of issue opening the stream
     */
    OutputStream createTransactionOutput(String storeName) throws IOException;

    /**
     * The flow of transactions coming from the redolog and to be processed. Transaction of different stores are interleaved in this flow. <br/>
     * The ids of the transactions in the flow is strictly increasing.
     */
    Publisher<TransactionInput> allTransactions();

    /**
     * Set the position of the flow to be retrieved through {@link #allTransactions()} at the provided transaction id. <br/>
     * The first transaction received in the flow will be the one folowing the provided id.
     */
    void seekTransaction(BigInteger latestProcessedTransactionId);

    /**
     * Dispose any resource the store may have open.
     */
    default void close() {}

    /**
     * An access to the content of a transaction
     */
    interface TransactionInput {
        /**
         * The store in which the transaction has to be applied
         */
        String storeName();

        /**
         * The content of the transaction
         */
        InputStream getInputStream() throws IOException;

        /**
         * The unique id of the transaction.
         */
        BigInteger getTransactionId();
    }
}
