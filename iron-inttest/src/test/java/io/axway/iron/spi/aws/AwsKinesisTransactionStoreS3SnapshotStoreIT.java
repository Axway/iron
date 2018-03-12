package io.axway.iron.spi.aws;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import io.axway.iron.spi.SpiTest;
import io.axway.iron.spi.jackson.JacksonSerializer;

import static io.axway.iron.spi.aws.AwsTestHelper.*;


public class AwsKinesisTransactionStoreS3SnapshotStoreIT extends BaseInttest {

    private String m_storeName;

    @BeforeMethod
    public void createBucketAndStream() {
        m_storeName = createRandomStoreName();
        m_configuration.setProperty(AwsTestHelper.S3_BUCKET_NAME, m_storeName);
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

        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        SpiTest.checkThatCreateCompanySequenceIsRight(buildAwsKinesisTransactionStoreFactory(m_configuration), jacksonSerializer,
                                                      buildAwsS3SnapshotStoreFactory(m_configuration), jacksonSerializer, m_storeName);
    }

    @Test(enabled = false)
    public void shouldListSnapshotsReturnTheRightNumberOfSnapshots() throws Exception {
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        SpiTest.checkThatListSnapshotsReturnTheRightNumberOfSnapshots(buildAwsKinesisTransactionStoreFactory(m_configuration), jacksonSerializer,
                                                                      buildAwsS3SnapshotStoreFactory(m_configuration), jacksonSerializer, m_storeName);
    }

    @Test(enabled = false)
    public void shouldRetrieveCommandsFromSnapshotStoreAndNotFromTransactionStore() throws Exception {
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        SpiTest.checkThatCommandIsExecutedFromSnapshotStoreNotFromTransactionStore(buildAwsKinesisTransactionStoreFactory(m_configuration), jacksonSerializer,
                                                                                   buildAwsS3SnapshotStoreFactory(m_configuration), jacksonSerializer,
                                                                                   m_storeName);
    }
}
