package io.axway.iron.spi.aws.s3;

import org.testng.annotations.Test;
import io.axway.iron.spi.SpiTestHelper;
import io.axway.iron.spi.aws.BaseInttest;
import io.axway.iron.spi.file.FileTestHelper;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static io.axway.iron.spi.aws.AwsTestHelper.*;
import static io.axway.iron.spi.file.FileTestHelper.buildFileTransactionStoreFactory;
import static io.axway.iron.spi.jackson.JacksonTestHelper.*;

/**
 * Test FileTransactionStore and S3SnapshotStore
 */
public class AwsS3SnapshotIT extends BaseInttest {

    @Test(enabled = false)
    public void shouldCreateCompanySequenceBeRight() throws Exception {
        String bucketName = initS3Configuration();
        createS3Bucket(bucketName);
        SnapshotStoreFactory s3SnapshotStoreFactory = buildAwsS3SnapshotStoreFactory(m_configuration);
        TransactionStoreFactory ironFileStoreFactory = buildFileTransactionStoreFactory(getIronSpiAwsInttestFilePath());

        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
        String storeName = createRandomStoreName();
        try {
            SpiTestHelper.checkThatCreateCompanySequenceIsRight(ironFileStoreFactory, transactionSerializer,    //
                                                                s3SnapshotStoreFactory, snapshotSerializer,  //
                                                                storeName);
        } finally {
            deleteS3Bucket(bucketName);
        }
    }

    @Test(enabled = false)
    public void shouldListSnapshotsReturnTheRightNumberOfSnapshotsSample() throws Exception {
        String storeName = createRandomStoreName();

        TransactionStoreFactory transactionStoreFactory = buildFileTransactionStoreFactory(getIronSpiAwsInttestFilePath());
        SnapshotStoreFactory snapshotStoreFactory = FileTestHelper.buildFileSnapshotStoreFactory(getIronSpiAwsInttestFilePath());

        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();

        SpiTestHelper.checkThatListSnapshotsReturnTheRightNumberOfSnapshots(transactionStoreFactory, transactionSerializer, //
                                                                            snapshotStoreFactory, snapshotSerializer, //
                                                                            storeName);
    }

    @Test(enabled = false)
    public void shouldListSnapshotsReturnTheRightNumberOfSnapshots() throws Exception {
        String bucketName = initS3Configuration();
        createS3Bucket(bucketName);

        SnapshotStoreFactory s3SnapshotStoreFactory = buildAwsS3SnapshotStoreFactory(m_configuration);
        TransactionStoreFactory transactionStoreFactory = buildFileTransactionStoreFactory(getIronSpiAwsInttestFilePath());

        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();

        String storeName = createRandomStoreName();
        try {
            SpiTestHelper.checkThatListSnapshotsReturnTheRightNumberOfSnapshots(transactionStoreFactory, transactionSerializer,    //
                                                                                s3SnapshotStoreFactory, snapshotSerializer,  //
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
        m_configuration.setProperty(S3_BUCKET_NAME, bucketName);
        return bucketName;
    }

    private String initDirectoryName() {
        String directoryName = createRandomDirectoryName();
        m_configuration.get(S3_DIRECTORY_NAME);
        return directoryName;
    }
}
