package io.axway.iron.spi.serializer;

import java.io.*;
import io.axway.iron.spi.model.transaction.SerializableTransaction;

/**
 * Transaction serialization interface.<br>
 * It reads/writes transaction's commands with theirs parameters.
 */
public interface TransactionSerializer {
    /**
     * Serialize a transaction.
     *
     * @param out the stream where the serialized transaction must be written
     * @param transaction the transaction to be serialized
     * @throws IOException in case of errors when writing on the {@code out} stream
     */
    void serializeTransaction(OutputStream out, SerializableTransaction transaction) throws IOException;

    /**
     * Deserialize a transaction.
     *
     * @param in the transaction is to be read from this stream
     * @return the deserialized transaction
     * @throws IOException in case of errors when writing on the {@code in} stream
     */
    SerializableTransaction deserializeTransaction(InputStream in) throws IOException;
}
