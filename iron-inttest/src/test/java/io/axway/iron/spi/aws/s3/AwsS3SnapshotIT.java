package io.axway.iron.spi.aws.s3;

import org.testng.annotations.Test;
import io.axway.iron.core.spi.file.FileStoreFactory;
import io.axway.iron.spi.SpiTest;
import io.axway.iron.spi.aws.BaseInttest;
import io.axway.iron.spi.jackson.JacksonSerializer;

import static io.axway.iron.spi.aws.AwsProperties.*;

/**
 * Test FileTransactionStore and S3SnapshotStore
 */
public class AwsS3SnapshotIT extends BaseInttest {

    @Test(enabled = false)
    public void shouldCreateCompanySequenceBeRight() throws Exception {
        String bucketName = initS3Configuration();
        createS3Bucket(bucketName);
        AwsS3SnapshotStoreFactory s3SnapshotStoreFactory = new AwsS3SnapshotStoreFactory(m_configuration);
        FileStoreFactory ironFileStoreFactory = buildFileStoreFactory();
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        String storeName = createRandomStoreName();
        try {
            SpiTest.checkThatCreateCompanySequenceIsRight(ironFileStoreFactory, jacksonSerializer, s3SnapshotStoreFactory, jacksonSerializer, storeName);
        } finally {
            deleteS3Bucket(bucketName);
        }
    }

    @Test(enabled = false)
    public void shouldListSnapshotsReturnTheRightNumberOfSnapshotsSample() throws Exception {
        String storeName = initStoreName();
        FileStoreFactory ironFileStoreFactory = buildFileStoreFactory();
        JacksonSerializer jacksonSerializer = new JacksonSerializer();

        SpiTest.checkThatListSnapshotsReturnTheRightNumberOfSnapshots(ironFileStoreFactory, jacksonSerializer, ironFileStoreFactory, jacksonSerializer,
                                                                      storeName);
    }

    @Test(enabled = false)
    public void shouldListSnapshotsReturnTheRightNumberOfSnapshots() throws Exception {
        String bucketName = initS3Configuration();
        createS3Bucket(bucketName);

        AwsS3SnapshotStoreFactory s3SnapshotStoreFactory = new AwsS3SnapshotStoreFactory(m_configuration);
        FileStoreFactory ironFileStoreFactory = buildFileStoreFactory();
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        String storeName = initStoreName();
        try {
            SpiTest.checkThatListSnapshotsReturnTheRightNumberOfSnapshots(ironFileStoreFactory, jacksonSerializer, s3SnapshotStoreFactory, jacksonSerializer,
                                                                          storeName);
        } finally {
            deleteS3Bucket(bucketName);
        }
    }

    private String initS3Configuration() {
        initDirectoryName();
        return initBucketName();
    }

    private String initBucketName() {
        String bucketName = createRandomBucketName();
        m_configuration.setProperty(S3_BUCKET_NAME_KEY.getPropertyKey(), bucketName);
        return bucketName;
    }

    private String initStoreName() {
        String storeName = createRandomBucketName();
        return storeName;
    }

    private String initDirectoryName() {
        String directoryName = createRandomDirectoryName();
        m_configuration.setProperty(S3_DIRECTORY_NAME_KEY.getPropertyKey(), directoryName);
        return directoryName;
    }
}
