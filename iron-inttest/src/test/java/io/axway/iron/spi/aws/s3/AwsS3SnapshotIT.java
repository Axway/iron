package io.axway.iron.spi.aws.s3;

import java.util.*;
import java.util.function.*;
import org.testng.annotations.Test;
import io.axway.iron.spi.aws.BaseInttest;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

import static io.axway.iron.spi.SpiTestHelper.*;
import static io.axway.iron.spi.aws.AwsTestHelper.*;
import static io.axway.iron.spi.file.FileTestHelper.*;
import static io.axway.iron.spi.jackson.JacksonTestHelper.*;

/**
 * Test FileTransactionStore and S3SnapshotStore
 */
public class AwsS3SnapshotIT extends BaseInttest {

    @Test(enabled = false)
    public void shouldCreateCompanySequenceBeRight() throws Exception {
        String bucketName = initS3Configuration();
        createS3Bucket(bucketName);
        String factoryName = "shouldCreateCompanySequenceBeRight-" + UUID.randomUUID();
        Supplier<SnapshotStore> s3SnapshotStoreFactory = () -> buildAwsS3SnapshotStoreFactory(factoryName, m_configuration);
        Supplier<TransactionStore> ironFileStoreFactory = () -> buildFileTransactionStore(getIronSpiAwsInttestFilePath(), factoryName);

        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
        String storeName = createRandomStoreName();
        try {
            checkThatCreateCompanySequenceIsRight(ironFileStoreFactory, transactionSerializer,    //
                                                  s3SnapshotStoreFactory, snapshotSerializer,  //
                                                  storeName);
        } finally {
            deleteS3Bucket(bucketName);
        }
    }

    @Test(enabled = false)
    public void shouldListSnapshotsReturnTheRightNumberOfSnapshotsSample() throws Exception {
        String storeName = createRandomStoreName();

        String factoryName = "shouldListSnapshotsReturnTheRightNumberOfSnapshotsSample-" + UUID.randomUUID();
        TransactionStore transactionStore = buildFileTransactionStore(getIronSpiAwsInttestFilePath(), factoryName);
        SnapshotStore snapshotStore = buildFileSnapshotStore(getIronSpiAwsInttestFilePath(), factoryName);

        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();

        checkThatListSnapshotsReturnTheRightNumberOfSnapshots(transactionStore, transactionSerializer, //
                                                              snapshotStore, snapshotSerializer, //
                                                              storeName);
    }

    @Test(enabled = false)
    public void shouldListSnapshotsReturnTheRightNumberOfSnapshots() throws Exception {
        String bucketName = initS3Configuration();
        createS3Bucket(bucketName);

        String factoryName = "shouldListSnapshotsReturnTheRightNumberOfSnapshots-" + UUID.randomUUID();
        SnapshotStore s3SnapshotStore = buildAwsS3SnapshotStoreFactory(factoryName, m_configuration);
        TransactionStore transactionStore = buildFileTransactionStore(getIronSpiAwsInttestFilePath(), factoryName);

        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();

        String storeName = createRandomStoreName();
        try {
            checkThatListSnapshotsReturnTheRightNumberOfSnapshots(transactionStore, transactionSerializer,    //
                                                                  s3SnapshotStore, snapshotSerializer,  //
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
