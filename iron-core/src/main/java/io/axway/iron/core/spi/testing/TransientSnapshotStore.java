package io.axway.iron.core.spi.testing;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import com.google.common.collect.ImmutableList;
import io.axway.iron.spi.storage.SnapshotStore;

class TransientSnapshotStore implements SnapshotStore {

    @Override
    public OutputStream createSnapshotWriter(BigInteger transactionId) throws IOException {
        return new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                // discard all bytes
            }
        };
    }

    @Override
    public InputStream createSnapshotReader(BigInteger transactionId) throws IOException {
        throw new IOException("Snapshot for transaction id=" + transactionId + " has not been found");
    }

    @Override
    public List<BigInteger> listSnapshots() {
        return ImmutableList.of();
    }

    @Override
    public void deleteSnapshot(BigInteger transactionId) {
        // nothing to do
    }
}
