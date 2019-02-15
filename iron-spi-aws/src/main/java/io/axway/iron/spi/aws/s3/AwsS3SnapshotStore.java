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
    private final StoreLocker m_storeLocker;

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
        m_rootDirectoryName = String.format(ROOT_FORMAT, directoryName);
        m_storeLocker = new StoreLocker(m_amazonS3, m_bucketName, this::getSnapshotDirectoryName);
    }

    /**
     * WARNING : only for test
     */
    protected AwsS3SnapshotStore(AmazonS3 amazonS3, String bucketName, String rootDirectoryName, StoreLocker storeLocker) {
        m_amazonS3 = amazonS3;
        m_bucketName = bucketName;
        m_rootDirectoryName = rootDirectoryName;
        m_storeLocker = storeLocker;
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
    public void prePersistSnapshot(BigInteger transactionId) {
        m_storeLocker.lockStore(transactionId);
    }

    @Override
    public void postPersistSnapshot(BigInteger transactionId) {
        m_storeLocker.unlockStore(transactionId);
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
        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request().withBucketName(m_bucketName).withPrefix(snapshotDirectoryName);
        return listAllObjects(listObjectsRequest, ListObjectsV2Result::getObjectSummaries);
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
                .withPrefix(m_rootDirectoryName + "/")                //
                .withDelimiter("/");
        return listAllObjects(request, ListObjectsV2Result::getCommonPrefixes) //
                .stream().flatMap(prefix -> {
                    Matcher matcher = DIRECTORY_PATTERN.matcher(prefix);
                    if (matcher.matches()) {
                        final BigInteger transactionId = new BigInteger(matcher.group(1));
                        if (m_storeLocker.isStoreLocked(transactionId)) {
                            return Stream.empty();
                        } else {
                            return Stream.of(transactionId);
                        }
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

    static class StoreLocker {

        private static final String LOCK = "lock";

        private final AmazonS3 m_amazonS3;
        private final String m_bucketName;
        private final Function<BigInteger, String> m_getDirectory;

        StoreLocker(AmazonS3 amazonS3, String bucketName, Function<BigInteger, String> getDirectory) {
            m_amazonS3 = amazonS3;
            m_bucketName = bucketName;
            m_getDirectory = getDirectory;
        }

        void lockStore(BigInteger transactionId) {
            m_amazonS3.putObject(m_bucketName, getLockObject(transactionId), "locked");
        }

        void unlockStore(BigInteger transactionId) {
            m_amazonS3.deleteObject(m_bucketName, getLockObject(transactionId));
        }

        boolean isStoreLocked(BigInteger transactionId) {
            return m_amazonS3.doesObjectExist(m_bucketName, getLockObject(transactionId));
        }

        @Nonnull
        private String getLockObject(BigInteger transactionId) {
            return m_getDirectory.apply(transactionId) + LOCK;
        }
    }
}
