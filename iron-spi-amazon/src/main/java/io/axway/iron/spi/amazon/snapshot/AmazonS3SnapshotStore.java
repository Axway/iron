package io.axway.iron.spi.amazon.snapshot;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.stream.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.axway.iron.spi.storage.SnapshotStore;

class AmazonS3SnapshotStore implements SnapshotStore {
    private static final String DIRNAME_FORMAT = "%s/tx";
    private static final String FILENAME_FORMAT = DIRNAME_FORMAT + "/%020d.tx";
    private static final Pattern FILENAME_PATTERN = Pattern.compile("([0-9]{20})\\.tx");

    private final AmazonS3 m_amazonS3;
    private final String m_bucketName;
    private final String m_storeName;

    AmazonS3SnapshotStore(AmazonS3 amazonS3, String bucketName, String storeName) {
        m_amazonS3 = amazonS3;
        m_bucketName = bucketName;
        m_storeName = storeName;
    }

    @Override
    public OutputStream createSnapshotWriter(long transactionId) throws IOException {
        return new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();

                byte[] content = toByteArray();
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentType("application/json");
                metadata.setContentLength(content.length);
                m_amazonS3.putObject(m_bucketName, getSnapshotFileName(transactionId), new ByteArrayInputStream(content), metadata);
            }
        };
    }

    @Override
    public InputStream createSnapshotReader(long transactionId) throws IOException {
        S3Object object = m_amazonS3.getObject(m_bucketName, getSnapshotFileName(transactionId));
        if (object == null) {
            throw new RuntimeException(transactionId + " doesn't exists"); //TODO
        }

        return object.getObjectContent();
    }

    @Override
    public List<Long> listSnapshots() {
        return m_amazonS3.listObjectsV2(m_bucketName, getSnapshotDirName()).getObjectSummaries().stream() //
                .map(S3ObjectSummary::getKey).map(FILENAME_PATTERN::matcher) //
                .filter(Matcher::matches) //
                .map(matcher -> matcher.group(1)) //
                .map(Long::parseLong) //
                .collect(Collectors.toList());
    }

    @Override
    public void deleteSnapshot(long transactionId) {
        m_amazonS3.deleteObject(m_bucketName, getSnapshotFileName(transactionId));
    }

    private String getSnapshotDirName() {
        return String.format(DIRNAME_FORMAT, m_storeName);
    }

    private String getSnapshotFileName(long id) {
        return String.format(FILENAME_FORMAT, m_storeName, id);
    }
}
