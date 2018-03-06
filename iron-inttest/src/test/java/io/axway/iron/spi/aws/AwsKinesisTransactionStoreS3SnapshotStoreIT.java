package io.axway.iron.spi.aws;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import io.axway.iron.spi.SpiTest;
import io.axway.iron.spi.aws.kinesis.AwsKinesisTransactionStoreFactory;
import io.axway.iron.spi.aws.s3.AwsS3SnapshotStoreFactory;
import io.axway.iron.spi.jackson.JacksonSerializer;

import static io.axway.iron.spi.aws.AwsProperties.S3_BUCKET_NAME_KEY;

@Test(enabled = false)
public class AwsKinesisTransactionStoreS3SnapshotStoreIT extends BaseInttest {

    private String m_storeName;

    @BeforeMethod
    public void createBucketAndStream() {
        m_storeName = createRandomStoreName();
        m_configuration.setProperty(S3_BUCKET_NAME_KEY.getPropertyKey(), m_storeName);
        createStreamAndWaitActivation(m_storeName);
        createS3Bucket(m_storeName);
    }

    @AfterMethod
    public void deleteBucketAndStream() {
        deleteKinesisStream(m_storeName);
        deleteS3Bucket(m_storeName);
    }

    @Test
    public void shouldCreateCompanySequenceBeRight() throws Exception {

        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        SpiTest.checkThatCreateCompanySequenceIsRight(new AwsKinesisTransactionStoreFactory(m_configuration), jacksonSerializer,
                                                      new AwsS3SnapshotStoreFactory(m_configuration), jacksonSerializer, m_storeName);
    }

    @Test
    public void shouldListSnapshotsReturnTheRightNumberOfSnapshots() throws Exception {
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        SpiTest.checkThatListSnapshotsReturnTheRightNumberOfSnapshots(new AwsKinesisTransactionStoreFactory(m_configuration), jacksonSerializer,
                                                                      new AwsS3SnapshotStoreFactory(m_configuration), jacksonSerializer, m_storeName);
    }

    @Test
    public void shouldRetrieveCommandsFromSnapshotStoreAndNotFromTransactionStore() throws Exception {
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        SpiTest.checkThatCommandIsExecutedFromSnapshotStoreNotFromTransactionStore(new AwsKinesisTransactionStoreFactory(m_configuration), jacksonSerializer,
                                                                                   new AwsS3SnapshotStoreFactory(m_configuration), jacksonSerializer,
                                                                                   m_storeName);
    }
}
