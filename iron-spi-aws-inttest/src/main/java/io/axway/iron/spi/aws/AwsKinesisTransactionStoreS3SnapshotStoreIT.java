package io.axway.iron.spi.aws;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import io.axway.iron.sample.Sample;
import io.axway.iron.spi.aws.kinesis.AwsKinesisTestUtils;
import io.axway.iron.spi.aws.kinesis.KinesisTransactionStoreFactory;
import io.axway.iron.spi.aws.s3.AwsS3SnapshotStoreFactory;
import io.axway.iron.spi.jackson.JacksonSerializer;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static io.axway.iron.spi.aws.kinesis.AwsKinesisTestUtils.buildTestAwsKinesisTransactionStoreFactory;
import static io.axway.iron.spi.aws.s3.AwsS3TestUtils.buildTestAwsS3SnapshotStoreFactory;

public class AwsKinesisTransactionStoreS3SnapshotStoreIT {

    @DataProvider(name = "stores")
    public Object[][] providesStores() {
        KinesisTransactionStoreFactory awsKinesisTransactionStoreFactory = buildTestAwsKinesisTransactionStoreFactory();
        AwsS3SnapshotStoreFactory awsS3SnapshotStoreFactory = buildTestAwsS3SnapshotStoreFactory();

        return new Object[][]{ //
                {awsKinesisTransactionStoreFactory, awsS3SnapshotStoreFactory}, //
        };
    }

    @Test(dataProvider = "stores")
    public void shouldCreateCompanySequenceBeRight(TransactionStoreFactory transactionStoreFactory, SnapshotStoreFactory snapshotStoreFactory)
            throws Exception {
        String storeName = AwsKinesisTestUtils.createStreamAndWaitActivationWithRandomName();
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        Sample.checkThatCreateCompanySequenceIsRight(transactionStoreFactory, jacksonSerializer, snapshotStoreFactory, jacksonSerializer, storeName);
    }

    @Test(dataProvider = "stores")
    public void testSnapshotStore(TransactionStoreFactory transactionStoreFactory, SnapshotStoreFactory snapshotStoreFactory) throws Exception {
        String storeName = AwsKinesisTestUtils.createStreamAndWaitActivationWithRandomName();
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        Sample.checkThatListSnapshotsReturnTheRightNumberOfSnapshots(transactionStoreFactory, jacksonSerializer, snapshotStoreFactory, jacksonSerializer,
                                                                     storeName);
    }

    @Test(dataProvider = "stores")
    public void shouldRetrieveCommandsFromSnapshotStoreAndNotFromTransactionStore(TransactionStoreFactory transactionStoreFactory,
                                                                                  SnapshotStoreFactory snapshotStoreFactory) throws Exception {
        String storeName = AwsKinesisTestUtils.createStreamAndWaitActivationWithRandomName();
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        Sample.checkThatCommandIsExecutedFromSnapshotStoreNotFromTransactionStore(transactionStoreFactory, jacksonSerializer, snapshotStoreFactory,
                                                                                  jacksonSerializer, storeName);
    }
}
