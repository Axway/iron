package io.axway.iron.spi.aws;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import io.axway.iron.spi.SpiTest;
import io.axway.iron.spi.aws.kinesis.AwsKinesisTransactionStoreFactory;
import io.axway.iron.spi.aws.s3.AwsS3SnapshotStoreFactory;
import io.axway.iron.spi.jackson.JacksonSerializer;

import static io.axway.iron.spi.aws.AwsProperties.*;

public class AwsKinesisTransactionStoreS3SnapshotStoreIT extends BaseInttest {

    private String m_bucketName;
    private String m_storeName;

    @BeforeMethod
    public void createBucketAndStream() {
        m_bucketName = createRandomBucketName();
        m_storeName = createRandomStoreName();
        String directoryName = createRandomDirectoryName();
        m_configuration.setProperty(S3_DIRECTORY_NAME_KEY.getPropertyKey(), directoryName);
        m_configuration.setProperty(S3_BUCKET_NAME_KEY.getPropertyKey(), m_bucketName);
        createStreamAndWaitActivation(m_storeName);
        createS3Bucket(m_bucketName);
    }

    @AfterMethod
    public void deleteBucketAndStream() {
        deleteKinesisStream(m_storeName);
        deleteS3Bucket(m_bucketName);
    }

    @Test(enabled = false)
    public void shouldCreateCompanySequenceBeRight() throws Exception {
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        SpiTest.checkThatCreateCompanySequenceIsRight(new AwsKinesisTransactionStoreFactory(m_configuration), jacksonSerializer,
                                                      new AwsS3SnapshotStoreFactory(m_configuration), jacksonSerializer, m_storeName);
    }

    @Test(enabled = false)
    public void shouldListSnapshotsReturnTheRightNumberOfSnapshots() throws Exception {
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        SpiTest.checkThatListSnapshotsReturnTheRightNumberOfSnapshots(new AwsKinesisTransactionStoreFactory(m_configuration), jacksonSerializer,
                                                                      new AwsS3SnapshotStoreFactory(m_configuration), jacksonSerializer, m_storeName);
    }

    @Test(enabled = false)
    public void shouldRetrieveCommandsFromSnapshotStoreAndNotFromTransactionStore() throws Exception {
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        SpiTest.checkThatCommandIsExecutedFromSnapshotStoreNotFromTransactionStore(new AwsKinesisTransactionStoreFactory(m_configuration), jacksonSerializer,
                                                                                   new AwsS3SnapshotStoreFactory(m_configuration), jacksonSerializer,
                                                                                   m_storeName);
    }
}
