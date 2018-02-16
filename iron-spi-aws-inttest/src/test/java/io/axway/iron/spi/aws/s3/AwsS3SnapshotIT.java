package io.axway.iron.spi.aws.s3;

import org.testng.annotations.Test;
import io.axway.iron.core.spi.file.FileStoreFactory;
import io.axway.iron.sample.SpiTest;
import io.axway.iron.spi.aws.BaseInttest;
import io.axway.iron.spi.jackson.JacksonSerializer;

/**
 * Test FileTransactionStore and S3SnapshotStore
 */
public class AwsS3SnapshotIT extends BaseInttest {

    @Test
    public void shouldCreateCompanySequenceBeRight() throws Exception {
        String storeName = initStoreName();
        createS3Bucket(storeName);
        AwsS3SnapshotStoreFactory s3SnapshotStoreFactory = new AwsS3SnapshotStoreFactory(m_configuration);
        FileStoreFactory ironFileStoreFactory = buildFileStoreFactory();
        JacksonSerializer jacksonSerializer = new JacksonSerializer();

        try {
            SpiTest.checkThatCreateCompanySequenceIsRight(ironFileStoreFactory, jacksonSerializer, s3SnapshotStoreFactory, jacksonSerializer, storeName);
        } finally {
            deleteS3Bucket(storeName);
        }
    }

    @Test
    public void shouldListSnapshotsReturnTheRightNumberOfSnapshotsSample() throws Exception {
        String storeName = initStoreName();
        FileStoreFactory ironFileStoreFactory = buildFileStoreFactory();
        JacksonSerializer jacksonSerializer = new JacksonSerializer();

        SpiTest.checkThatListSnapshotsReturnTheRightNumberOfSnapshots(ironFileStoreFactory, jacksonSerializer, ironFileStoreFactory, jacksonSerializer,
                                                                      storeName);
    }

    @Test
    public void shouldListSnapshotsReturnTheRightNumberOfSnapshots() throws Exception {
        String storeName = initStoreName();
        createS3Bucket(storeName);
        AwsS3SnapshotStoreFactory s3SnapshotStoreFactory = new AwsS3SnapshotStoreFactory(m_configuration);
        FileStoreFactory ironFileStoreFactory = buildFileStoreFactory();
        JacksonSerializer jacksonSerializer = new JacksonSerializer();

        try {
            SpiTest.checkThatListSnapshotsReturnTheRightNumberOfSnapshots(ironFileStoreFactory, jacksonSerializer, s3SnapshotStoreFactory, jacksonSerializer,
                                                                          storeName);
        } finally {
            deleteS3Bucket(storeName);
        }
    }

    private String initStoreName() {
        String storeName = createRandomStoreName();
        m_configuration.setProperty(AwsS3Properties.S3_BUCKET_NAME_KEY.getPropertyKey(), storeName);
        return storeName;
    }
}
