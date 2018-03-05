package io.axway.iron.spi.aws.s3;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.regex.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.axway.iron.spi.storage.SnapshotStore;

import static java.util.stream.Collectors.*;

/**
 * AWS S3 SnapshotStore.
 */
class AwsS3SnapshotStore implements SnapshotStore {

    private static final String DIRECTORYNAME_FORMAT = "%s/snapshot";
    private static final String FILENAME_FORMAT = DIRECTORYNAME_FORMAT + "/%d.snapshot";
    private static final Pattern FILENAME_PATTERN = Pattern.compile("([0-9]+)\\.snapshot");
    private static final String APPLICATION_JSON_CONTENT_TYPE = "application/json";

    private final AmazonS3 m_amazonS3;
    private final String m_bucketName;
    private final String m_storeName;
    private final String m_snapshotDirName;

    /**
     * Create an AWS S3 SnapshotStore. The bucket named "bucketName" must exist.
     *
     * @param amazonS3 AmazonS3 client
     * @param bucketName Bucket name
     * @param storeName Store name
     */
    AwsS3SnapshotStore(AmazonS3 amazonS3, String bucketName, String storeName) {
        m_amazonS3 = amazonS3;
        m_bucketName = bucketName;
        m_storeName = storeName;
        m_snapshotDirName = getSnapshotDirectoryName();
    }

    @Override
    public OutputStream createSnapshotWriter(BigInteger transactionId) throws IOException {
        return new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();
                byte[] content = toByteArray();
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentType(APPLICATION_JSON_CONTENT_TYPE);
                metadata.setContentLength(content.length);
                m_amazonS3.putObject(new PutObjectRequest(m_bucketName, getSnapshotFileName(transactionId), new ByteArrayInputStream(content), metadata));
            }
        };
    }

    @Override
    public InputStream createSnapshotReader(BigInteger transactionId) throws IOException {
        S3Object object = m_amazonS3.getObject(m_bucketName, getSnapshotFileName(transactionId));
        if (object == null) {
            throw new AwsS3Exception("Snapshot doesn't exist", args -> args.add("bucketName", m_bucketName).add("transactionId", transactionId));
        }
        return object.getObjectContent();
    }

    @Override
    public List<BigInteger> listSnapshots() {
        List<S3ObjectSummary> objectSummaries = m_amazonS3.listObjectsV2(m_bucketName, m_snapshotDirName).getObjectSummaries();
        return objectSummaries.stream() //
                .map(S3ObjectSummary::getKey) //
                .map(key -> key.replace(m_snapshotDirName + "/", "")) //
                .map(FILENAME_PATTERN::matcher) //
                .filter(Matcher::matches) //
                .map(matcher -> matcher.group(1)) //
                .map(BigInteger::new) //
                .collect(toList());
    }

    @Override
    public void deleteSnapshot(BigInteger transactionId) {
        m_amazonS3.deleteObject(m_bucketName, getSnapshotFileName(transactionId));
    }

    /**
     * Return the snapshot directory name.
     *
     * @return the snapshot directory name
     */
    private String getSnapshotDirectoryName() {
        return String.format(DIRECTORYNAME_FORMAT, m_storeName);
    }

    /**
     * Return the snapshot file name based on the transactionId.
     *
     * @param transactionId the transactionId
     * @return the snapshot file name
     */
    private String getSnapshotFileName(BigInteger transactionId) {
        return String.format(FILENAME_FORMAT, m_storeName, transactionId);
    }
}
