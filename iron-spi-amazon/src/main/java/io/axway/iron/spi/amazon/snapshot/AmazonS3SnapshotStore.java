package io.axway.iron.spi.amazon.snapshot;

import java.io.*;
import java.util.*;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import io.axway.iron.spi.storage.SnapshotStore;

class AmazonS3SnapshotStore implements SnapshotStore {
    private final AmazonS3Client m_amazonS3Client;
    private final String m_storeName;

    AmazonS3SnapshotStore(AmazonS3Client amazonS3Client, String storeName) {
        m_amazonS3Client = amazonS3Client;
        m_storeName = storeName;

        Bucket bucket = m_amazonS3Client.createBucket(storeName);
    }

    @Override
    public OutputStream createSnapshotWriter(long transactionId) throws IOException {
        return new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();

                m_amazonS3Client.putObject(m_storeName, "" + transactionId, new ByteArrayInputStream(toByteArray()), null);
            }
        };
    }

    @Override
    public InputStream createSnapshotReader(long transactionId) throws IOException {
        return null;  // TODO implement method
    }

    @Override
    public List<Long> listSnapshots() {
        return null;  // TODO implement method
    }

    @Override
    public void deleteSnapshot(long transactionId) {
        // TODO implement method
    }
}
