package io.axway.iron.spi.aws.s3;

import org.testng.annotations.Test;
import io.axway.iron.spi.SpiTest;
import io.axway.iron.spi.aws.AwsTestHelper;
import io.axway.iron.spi.aws.BaseInttest;
import io.axway.iron.spi.file.FileTestHelper;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static io.axway.iron.spi.aws.AwsTestHelper.buildAwsS3SnapshotStoreFactory;
import static io.axway.iron.spi.file.FileTestHelper.buildFileTransactionStoreFactory;
import static io.axway.iron.spi.jackson.JacksonTestHelper.*;

/**
 * Test FileTransactionStore and S3SnapshotStore
 */
public class AwsS3SnapshotIT extends BaseInttest {

    @Test(enabled = false)
    public void shouldCreateCompanySequenceBeRight() throws Exception {
        String storeName = initStoreName();
        createS3Bucket(storeName);
        SnapshotStoreFactory s3SnapshotStoreFactory = buildAwsS3SnapshotStoreFactory(m_configuration);
        TransactionStoreFactory ironFileStoreFactory = buildFileTransactionStoreFactory(getIronSpiAwsInttestFilePath());

        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();

        try {
            SpiTest.checkThatCreateCompanySequenceIsRight(ironFileStoreFactory, transactionSerializer,    //
                                                          s3SnapshotStoreFactory, snapshotSerializer,  //
                                                          storeName);
        } finally {
            deleteS3Bucket(storeName);
        }
    }

    @Test(enabled = false)
    public void shouldListSnapshotsReturnTheRightNumberOfSnapshotsSample() throws Exception {
        String storeName = initStoreName();

        TransactionStoreFactory transactionStoreFactory = buildFileTransactionStoreFactory(getIronSpiAwsInttestFilePath());
        SnapshotStoreFactory snapshotStoreFactory = FileTestHelper.buildFileSnapshotStoreFactory(getIronSpiAwsInttestFilePath());

        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();

        SpiTest.checkThatListSnapshotsReturnTheRightNumberOfSnapshots(transactionStoreFactory, transactionSerializer, //
                                                                      snapshotStoreFactory, snapshotSerializer, //
                                                                      storeName);
    }

    @Test(enabled = false)
    public void shouldListSnapshotsReturnTheRightNumberOfSnapshots() throws Exception {
        String storeName = initStoreName();
        createS3Bucket(storeName);
        SnapshotStoreFactory s3SnapshotStoreFactory = buildAwsS3SnapshotStoreFactory(m_configuration);
        TransactionStoreFactory transactionStoreFactory = buildFileTransactionStoreFactory(getIronSpiAwsInttestFilePath());

        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();

        try {
            SpiTest.checkThatListSnapshotsReturnTheRightNumberOfSnapshots(transactionStoreFactory, transactionSerializer,    //
                                                                          s3SnapshotStoreFactory, snapshotSerializer,  //
                                                                          storeName);
        } finally {
            deleteS3Bucket(storeName);
        }
    }

    private String initStoreName() {
        String storeName = createRandomStoreName();
        m_configuration.setProperty(AwsTestHelper.S3_BUCKET_NAME, storeName);
        return storeName;
    }
}
