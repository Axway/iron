package io.axway.iron.spi.s3;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.regex.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.axway.iron.spi.storage.SnapshotStore;

import static com.google.common.base.Preconditions.checkState;
import static io.axway.iron.spi.s3.AwsS3Utils.doesBucketExist;
import static java.util.stream.Collectors.*;

class AmazonS3SnapshotStore implements SnapshotStore {

    private static final String DIRNAME_FORMAT = "%s/tx";
    private static final String FILENAME_FORMAT = DIRNAME_FORMAT + "/%d.tx";
    private static final Pattern FILENAME_PATTERN = Pattern.compile("([0-9]+)\\.tx");
    private final AmazonS3 m_amazonS3;
    private final String m_bucketName;
    private String m_storeName;
    private String m_snapshotDirName;

    public AmazonS3SnapshotStore(AmazonS3 amazonS3, String bucketName, String storeName) {
        m_amazonS3 = amazonS3;
        m_bucketName = bucketName;
        m_storeName = storeName;
        m_snapshotDirName = getSnapshotDirName();
        checkState(doesBucketExist(m_amazonS3, bucketName), "Bucket %s does not exist.", bucketName);
    }

    @Override
    public OutputStream createSnapshotWriter(BigInteger transactionId) throws IOException {
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
    public InputStream createSnapshotReader(BigInteger transactionId) throws IOException {
        S3Object object = m_amazonS3.getObject(m_bucketName, getSnapshotFileName(transactionId));
        if (object == null) {
            throw new RuntimeException(transactionId + " doesn't exists");
        }
        return object.getObjectContent();
    }

    @Override
    public List<BigInteger> listSnapshots() {
        List<S3ObjectSummary> objectSummaries = m_amazonS3.listObjectsV2(m_bucketName, m_snapshotDirName).getObjectSummaries();
        return objectSummaries.stream() //
                .map(S3ObjectSummary::getKey) //
                .map(key -> key.replace(m_snapshotDirName + "/", "")).map(FILENAME_PATTERN::matcher) //
                .filter(Matcher::matches) //
                .map(matcher -> matcher.group(1)) //
                .map(BigInteger::new) //
                .collect(toList());
    }

    @Override
    public void deleteSnapshot(BigInteger transactionId) {
        m_amazonS3.deleteObject(m_bucketName, getSnapshotFileName(transactionId));
    }

    private String getSnapshotDirName() {
        return String.format(DIRNAME_FORMAT, m_storeName);
    }

    private String getSnapshotFileName(BigInteger id) {
        return String.format(FILENAME_FORMAT, m_storeName, id);
    }
}
