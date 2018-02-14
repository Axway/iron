package io.axway.iron.spi.aws.kinesis;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import io.axway.iron.sample.Sample;
import io.axway.iron.spi.aws.BaseInttest;
import io.axway.iron.spi.aws.s3.AwsS3Properties;
import io.axway.iron.spi.aws.s3.AwsS3SnapshotStoreFactory;
import io.axway.iron.spi.jackson.JacksonSerializer;

public class AwsKinesisTransactionStoreS3SnapshotStoreIT extends BaseInttest {

    private String m_storeName;

    @BeforeMethod
    public void createBucketAndStream() {
        m_storeName = createRandomStoreName();
        m_configuration.setProperty(AwsS3Properties.S3_BUCKET_NAME_KEY.getPropertyKey(), m_storeName);
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
        Sample.checkThatCreateCompanySequenceIsRight(new KinesisTransactionStoreFactory(m_configuration), jacksonSerializer,
                                                     new AwsS3SnapshotStoreFactory(m_configuration), jacksonSerializer, m_storeName);
    }

    @Test
    public void testSnapshotStore() throws Exception {
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        Sample.checkThatListSnapshotsReturnTheRightNumberOfSnapshots(new KinesisTransactionStoreFactory(m_configuration), jacksonSerializer,
                                                                     new AwsS3SnapshotStoreFactory(m_configuration), jacksonSerializer, m_storeName);
    }

    @Test
    public void shouldRetrieveCommandsFromSnapshotStoreAndNotFromTransactionStore() throws Exception {
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        Sample.checkThatCommandIsExecutedFromSnapshotStoreNotFromTransactionStore(new KinesisTransactionStoreFactory(m_configuration), jacksonSerializer,
                                                                                  new AwsS3SnapshotStoreFactory(m_configuration), jacksonSerializer,
                                                                                  m_storeName);
    }
}
