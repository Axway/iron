package io.axway.iron.spi.aws.s3;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.function.*;
import java.util.regex.*;
import java.util.stream.*;
import javax.annotation.*;
import org.reactivestreams.Publisher;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.axway.alf.exception.IllegalStateFormattedException;
import io.axway.iron.spi.storage.SnapshotStore;
import io.reactivex.Flowable;

import static io.axway.iron.spi.aws.s3.AwsS3Utils.*;
import static java.util.stream.Collectors.*;

/**
 * AWS S3 Snapshot Store Factory.
 */
public class AwsS3SnapshotStore implements SnapshotStore {
    // snapshot ids
    private static final String SNAPSHOTS_IDS_DIRECTORY_FORMAT = "%s/snapshot/ids"; //
    private static final String SNAPSHOTS_IDS_FILE_FORMAT = "%s/snapshot/ids/%d"; //
    private static final Pattern SNAPSHOTS_IDS_DIRECTORY_PATTERN = Pattern.compile(".*/snapshot/ids/(\\d+)");
    // snapshot data
    private static final String SNAPSHOT_DATA_ID_DIRECTORY_FORMAT = "%s/snapshot/data/%d"; //
    private static final String SNAPSHOT_DATA_ID_STORE_FORMAT = "%s/snapshot/data/%d/%s.snapshot";
    private static final Pattern SNAPSHOT_DATA_FILE_PATTERN = Pattern.compile(".*/snapshot/data/\\d+/(.+)\\.snapshot");

    private static final String APPLICATION_JSON_CONTENT_TYPE = "application/json";

    private final AmazonS3 m_amazonS3;
    private final String m_bucketName;
    private String m_directoryName;

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
    protected AwsS3SnapshotStore(String accessKey, String secretKey, String endpoint, Integer port, String region, String bucketName, String directoryName) {
        m_amazonS3 = buildS3Client(accessKey, secretKey, endpoint, port, region);
        m_bucketName = checkBucketIsAccessible(m_amazonS3, bucketName);
        m_directoryName = directoryName;
    }

    /**
     * WARNING : only for test
     */
    protected AwsS3SnapshotStore(AmazonS3 amazonS3, String bucketName, String directoryName) {
        m_amazonS3 = amazonS3;
        m_bucketName = bucketName;
        m_directoryName = directoryName;
    }

    @Override
    public SnapshotStoreWriter createSnapshotWriter(BigInteger transactionId) {
        return new SnapshotStoreWriter() {
            @Override
            public OutputStream getOutputStream(String storeName) {
                return new ByteArrayOutputStream() {
                    @Override
                    public void close() throws IOException {
                        super.close();
                        byte[] content = toByteArray();
                        ObjectMetadata metadata = new ObjectMetadata();
                        metadata.setContentType(APPLICATION_JSON_CONTENT_TYPE);
                        metadata.setContentLength(content.length);
                        m_amazonS3.putObject(
                                new PutObjectRequest(m_bucketName, getSnapshotDataFileName(transactionId, storeName), new ByteArrayInputStream(content),
                                                     metadata));
                    }
                };
            }

            @Override
            public void commit() {
                m_amazonS3.putObject(m_bucketName, getSnapshotIdFile(transactionId), "snapshot");
            }
        };
    }

    @Override
    public Publisher<StoreSnapshotReader> createSnapshotReader(BigInteger transactionId) {
        return Flowable.fromIterable(listSnapshotDataStoresForId(transactionId).
                stream().map(S3ObjectSummary::getKey).flatMap(key -> extractStoreName(key).
                map(storeName -> new StoreSnapshotReader() {
                    @Override
                    public String storeName() {
                        return storeName;
                    }

                    @Override
                    public InputStream inputStream() {
                        return m_amazonS3.getObject(m_bucketName, key).getObjectContent();
                    }
                })).collect(toList()));
    }

    private List<S3ObjectSummary> listSnapshotDataStoresForId(BigInteger transactionId) {
        String snapshotDataIdDirectory = getSnapshotDataIdDirectory(transactionId);
        ListObjectsV2Request storesRequest = new ListObjectsV2Request().withBucketName(m_bucketName).withPrefix(snapshotDataIdDirectory);
        return listAllObjects(storesRequest, ListObjectsV2Result::getObjectSummaries);
    }

    @Nonnull
    private <T> List<T> listAllObjects(ListObjectsV2Request listObjectsRequest, Function<ListObjectsV2Result, List<T>> listObjectsResultMapper) {
        List<T> result = new ArrayList<>();
        ListObjectsV2Result listObjectsResult;
        do {
            listObjectsResult = m_amazonS3.listObjectsV2(listObjectsRequest);
            result.addAll(listObjectsResultMapper.apply(listObjectsResult));
            //
            String continuationToken = listObjectsResult.getNextContinuationToken();
            listObjectsRequest.setContinuationToken(continuationToken);
        } while (listObjectsResult.isTruncated());
        return result;
    }

    @Override
    public List<BigInteger> listSnapshots() {
        ListObjectsV2Request request = new ListObjectsV2Request()     //
                .withBucketName(m_bucketName)                         //
                .withPrefix(getSnapshotIdsPrefix());
        return listAllObjects(request, ListObjectsV2Result::getObjectSummaries) //
                .stream().flatMap(this::getSnapshotId).collect(toList());
    }

    @Nonnull
    private Stream<BigInteger> getSnapshotId(S3ObjectSummary object) {
        return extractSnapshotId(object.getKey()).
                map(BigInteger::new);
    }

    @Override
    public void close() {
        m_amazonS3.shutdown();
    }

    @Override
    public void deleteSnapshot(BigInteger transactionId) {
        // delete snapshot/ids/1234
        m_amazonS3.deleteObject(m_bucketName, getSnapshotIdFile(transactionId));
        // delete snapshot/data/1234/xxx
        listSnapshotDataStoresForId(transactionId).
                forEach(summary -> m_amazonS3.deleteObject(m_bucketName, summary.getKey()));
        // delete snapshot/data/1234
        m_amazonS3.deleteObject(m_bucketName, getSnapshotDataIdDirectory(transactionId));
    }

    String getSnapshotIdsPrefix() {
        return String.format(SNAPSHOTS_IDS_DIRECTORY_FORMAT, m_directoryName) + "/";
    }

    String getSnapshotIdFile(BigInteger transactionId) {
        return String.format(SNAPSHOTS_IDS_FILE_FORMAT, m_directoryName, transactionId);
    }

    String getSnapshotDataFileName(BigInteger transactionId, String storeName) {
        return String.format(SNAPSHOT_DATA_ID_STORE_FORMAT, m_directoryName, transactionId, storeName);
    }

    Stream<String> extractStoreName(String key) {
        return SNAPSHOT_DATA_FILE_PATTERN.matcher(key).results().map(matchResult -> matchResult.group(1));
    }

    String getSnapshotDataIdDirectory(BigInteger transactionId) {
        return String.format(SNAPSHOT_DATA_ID_DIRECTORY_FORMAT, m_directoryName, transactionId);
    }

    Stream<String> extractSnapshotId(String key) {
        return SNAPSHOTS_IDS_DIRECTORY_PATTERN.matcher(key).results().map(matchResult -> matchResult.group(1));
    }
}
