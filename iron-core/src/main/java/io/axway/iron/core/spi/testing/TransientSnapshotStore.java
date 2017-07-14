package io.axway.iron.core.spi.testing;

import java.io.*;
import java.util.*;
import com.google.common.collect.ImmutableList;
import io.axway.iron.spi.storage.SnapshotStore;

class TransientSnapshotStore implements SnapshotStore {

    @Override
    public OutputStream createSnapshotWriter(long transactionId) throws IOException {
        return new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                // discard all bytes
            }
        };
    }

    @Override
    public InputStream createSnapshotReader(long transactionId) throws IOException {
        throw new IOException("Snapshot for transaction id=" + transactionId + " has not been found");
    }

    @Override
    public List<Long> listSnapshots() {
        return ImmutableList.of();
    }

    @Override
    public void deleteSnapshot(long transactionId) {
    }
}
