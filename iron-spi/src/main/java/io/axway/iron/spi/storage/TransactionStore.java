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
     *
     * @param storeName the name of the store in which the transaction is performed
     * @return An output stream into which the content of the transaction should be written
     * @throws IOException in case of issue opening the stream
     */
    OutputStream createTransactionOutput(String storeName) throws IOException;

    /**
     * The flow of transactions coming from the redolog and to be processed. Transaction of different stores are interleaved in this flow.
     * The ids of the transactions in the flow is strictly increasing.
     *
     * @return A publisher for the flow of transactions coming from the redolog
     */
    Publisher<TransactionInput> allTransactions();

    /**
     * Set the position of the flow to be retrieved through {@link #allTransactions()} at the provided transaction id.
     * The first transaction received in the flow will be the one following the provided id.
     *
     * @param latestProcessedTransactionId transaction id from which the flow of transactions should be resumed
     */
    void seekTransaction(BigInteger latestProcessedTransactionId);

    /**
     * Dispose any resource the store may have open.
     */
    default void close() {
        // default is to do nothing
    }

    /**
     * An access to the content of a transaction
     */
    interface TransactionInput {
        /**
         * The store in which the transaction has to be applied
         *
         * @return the name of the store in which this transaction has been stored
         */
        String storeName();

        /**
         * The content of the transaction
         *
         * @return the input stream to access the content of this transaction
         * @throws IOException if an I/O error occurs when retrieving the stream
         */
        InputStream getInputStream() throws IOException;

        /**
         * The unique id of the transaction.
         *
         * @return the id of the transaction
         */
        BigInteger getTransactionId();
    }
}
