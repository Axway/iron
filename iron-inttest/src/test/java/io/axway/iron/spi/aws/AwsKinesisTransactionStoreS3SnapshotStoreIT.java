package io.axway.iron.spi.aws;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import io.axway.iron.spi.SpiTest;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;

import static io.axway.iron.spi.aws.AwsTestHelper.*;
import static io.axway.iron.spi.jackson.JacksonTestHelper.*;

public class AwsKinesisTransactionStoreS3SnapshotStoreIT extends BaseInttest {

    private String m_storeName;

    @BeforeMethod
    public void createBucketAndStream() {
        m_storeName = createRandomStoreName();
        m_configuration.setProperty(S3_BUCKET_NAME, m_storeName);
        createStreamAndWaitActivation(m_storeName);
        createS3Bucket(m_storeName);
    }

    @AfterMethod
    public void deleteBucketAndStream() {
        deleteKinesisStream(m_storeName);
        deleteS3Bucket(m_storeName);
    }

    @Test(enabled = false)
    public void shouldCreateCompanySequenceBeRight() throws Exception {

        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();

        SpiTest.checkThatCreateCompanySequenceIsRight(buildAwsKinesisTransactionStoreFactory(m_configuration), transactionSerializer,
                                                      buildAwsS3SnapshotStoreFactory(m_configuration), snapshotSerializer, m_storeName);
    }

    @Test(enabled = false)
    public void shouldListSnapshotsReturnTheRightNumberOfSnapshots() throws Exception {
        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();

        SpiTest.checkThatListSnapshotsReturnTheRightNumberOfSnapshots(buildAwsKinesisTransactionStoreFactory(m_configuration), transactionSerializer,
                                                                      buildAwsS3SnapshotStoreFactory(m_configuration), snapshotSerializer, m_storeName);
    }

    @Test(enabled = false)
    public void shouldRetrieveCommandsFromSnapshotStoreAndNotFromTransactionStore() throws Exception {
        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();

        SpiTest.checkThatCommandIsExecutedFromSnapshotStoreNotFromTransactionStore(buildAwsKinesisTransactionStoreFactory(m_configuration),
                                                                                   transactionSerializer, buildAwsS3SnapshotStoreFactory(m_configuration),
                                                                                   snapshotSerializer, m_storeName);
    }
}
