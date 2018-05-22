package io.axway.iron.spi.aws.s3;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.regex.*;
import java.util.stream.*;
import org.reactivestreams.Publisher;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.axway.iron.spi.storage.SnapshotStore;
import io.reactivex.Flowable;

import static io.axway.iron.spi.aws.s3.AwsS3Utils.*;
import static java.util.stream.Collectors.*;

/**
 * AWS S3 Snapshot Store Factory.
 */
public class AwsS3SnapshotStore implements SnapshotStore {

    private static final String ROOT_FORMAT = "%s/snapshot"; //
    private static final String DIRECTORY_NAME_FORMAT = "%s/%d/"; //
    private static final String FILENAME_FORMAT = DIRECTORY_NAME_FORMAT + "%s.snapshot";
    private static final Pattern DIRECTORY_PATTERN = Pattern.compile(".*/snapshot/(\\d+)/");
    private static final Pattern FILE_PATTERN = Pattern.compile(".*/snapshot/\\d+/(.+)\\.snapshot");
    private static final String APPLICATION_JSON_CONTENT_TYPE = "application/json";

    private final AmazonS3 m_amazonS3;
    private final String m_bucketName;
    private final String m_rootDirectoryName;

    /**
     * Create a AwsS3SnapshotStore with some properties set to configure S3 and the bucket :
     *
     * @param accessKey aws access key (optional+)
     * @param secretKey aws secret key (optional+)
     * @param endpoint s3 endpoint (optional*)
     * @param port s3 port (optional*)
     * @param region aws region (optional*)
     * @param bucketName S3 bucket name (mandatory)
     * @param directoryName directory name prefix (optional)
     * (+) to configure the access, both access key and secret key must be provided.
     * (*) to configure the endpoint URL, the endpoint, the port and the region must be provided.
     */
    AwsS3SnapshotStore(String accessKey, String secretKey, String endpoint, Integer port, String region, String bucketName,
                       String directoryName) {
        m_amazonS3 = buildS3Client(accessKey, secretKey, endpoint, port, region);
        m_bucketName = checkBucketIsAccessible(m_amazonS3, bucketName);
        m_rootDirectoryName = String.format(ROOT_FORMAT, directoryName);
    }

    /**
     * WARNING : only for test
     */
    AwsS3SnapshotStore(AmazonS3 amazonS3, String bucketName, String rootDirectoryName) {
        m_amazonS3 = amazonS3;
        m_bucketName = bucketName;
        m_rootDirectoryName = rootDirectoryName;
    }

    @Override
    public OutputStream createSnapshotWriter(String storeName, BigInteger transactionId) {
        return new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();
                byte[] content = toByteArray();
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentType(APPLICATION_JSON_CONTENT_TYPE);
                metadata.setContentLength(content.length);
                m_amazonS3.putObject(
                        new PutObjectRequest(m_bucketName, getSnapshotFileName(transactionId, storeName), new ByteArrayInputStream(content), metadata));
            }
        };
    }

    @Override
    public Publisher<StoreSnapshotReader> createSnapshotReader(BigInteger transactionId) {
        return Flowable.fromIterable(listSnapshotFilesForId(transactionId)) //
                .flatMap(summary -> {
                    Matcher matcher = FILE_PATTERN.matcher(summary.getKey());
                    if (matcher.matches()) {
                        String store = matcher.group(1);
                        return Flowable.just(new StoreSnapshotReader() {
                            @Override
                            public String storeName() {
                                return store;
                            }

                            @Override
                            public InputStream inputStream() {
                                return m_amazonS3.getObject(m_bucketName, summary.getKey()).getObjectContent();
                            }
                        });
                    } else {
                        return Flowable.empty();
                    }
                });
    }

    private List<S3ObjectSummary> listSnapshotFilesForId(BigInteger transactionId) {
        String snapshotDirectoryName = getSnapshotDirectoryName(transactionId);
        return m_amazonS3.listObjectsV2(m_bucketName, snapshotDirectoryName).getObjectSummaries();
    }

    @Override
    public List<BigInteger> listSnapshots() {
        ListObjectsV2Request request = new ListObjectsV2Request()     //
                .withBucketName(m_bucketName)                         //
                .withPrefix(m_rootDirectoryName + "/")                //
                .withDelimiter("/");
        return m_amazonS3.listObjectsV2(request) //
                .getCommonPrefixes().stream()
                .flatMap(prefix -> {
                    Matcher matcher = DIRECTORY_PATTERN.matcher(prefix);
                    if (matcher.matches()) {
                        return Stream.of(new BigInteger(matcher.group(1)));
                    } else {
                        return Stream.empty();
                    }
                }).collect(toList());
    }

    @Override
    public void close() {
        m_amazonS3.shutdown();
    }

    @Override
    public void deleteSnapshot(BigInteger transactionId) {
        listSnapshotFilesForId(transactionId).forEach(summary -> m_amazonS3.deleteObject(m_bucketName, summary.getKey()));
        m_amazonS3.deleteObject(m_bucketName, getSnapshotDirectoryName(transactionId));
    }

    private String getSnapshotDirectoryName(BigInteger transactionId) {
        return String.format(DIRECTORY_NAME_FORMAT, m_rootDirectoryName, transactionId);
    }

    private String getSnapshotFileName(BigInteger transactionId, String storeName) {
        return String.format(FILENAME_FORMAT, m_rootDirectoryName, transactionId, storeName);
    }
}
