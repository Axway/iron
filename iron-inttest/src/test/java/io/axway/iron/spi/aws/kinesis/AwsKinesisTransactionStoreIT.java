package io.axway.iron.spi.aws.kinesis;

import org.testng.annotations.Test;
import io.axway.iron.spi.SpiTest;
import io.axway.iron.spi.aws.BaseInttest;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static io.axway.iron.spi.aws.AwsTestHelper.buildAwsKinesisTransactionStoreFactory;
import static io.axway.iron.spi.file.FileTestHelper.*;
import static io.axway.iron.spi.jackson.JacksonTestHelper.*;

/**
 * Test KinesisTransactionStore and FileSnapshotStore
 */
public class AwsKinesisTransactionStoreIT extends BaseInttest {

    @Test(enabled = false)
    public void shouldCreateCompanySequenceBeRight() throws Exception {
        String randomStoreName = createRandomStoreName();
        createStreamAndWaitActivation(randomStoreName);
        SnapshotStoreFactory fileSnapshotStoreFactory = buildFileSnapshotStoreFactory(getIronSpiAwsInttestFilePath(), null);
        TransactionStoreFactory awsKinesisTransactionStoreFactory = buildAwsKinesisTransactionStoreFactory(m_configuration);

        try {
            TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
            SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
            SpiTest.checkThatCreateCompanySequenceIsRight(awsKinesisTransactionStoreFactory, transactionSerializer,  //
                                                          fileSnapshotStoreFactory, snapshotSerializer,              //
                                                          randomStoreName);
        } finally {
            deleteKinesisStream(randomStoreName);
        }
    }

    @Test(enabled = false)
    public void shouldRetrieveCommandsFromSnapshotFileStoreAndNotFromTransactionFileStore() throws Exception {
        String randomStoreName = createRandomStoreName();
        SnapshotStoreFactory fileSnapshotStoreFactory = buildFileSnapshotStoreFactory(getIronSpiAwsInttestFilePath(), null);
        TransactionStoreFactory fileTransactionStoreFactory = buildFileTransactionStoreFactory(getIronSpiAwsInttestFilePath(), null);

        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
        SpiTest.checkThatCommandIsExecutedFromSnapshotStoreNotFromTransactionStore(fileTransactionStoreFactory, transactionSerializer,         //
                                                                                   fileSnapshotStoreFactory, snapshotSerializer, //
                                                                                   randomStoreName);
    }

    @Test(enabled = false)
    public void shouldRetrieveCommandsFromSnapshotFileStoreAndNotFromTransactionKinesisStoreSample() throws Exception {
        String randomStoreName = createRandomStoreName();
        createStreamAndWaitActivation(randomStoreName);
        SnapshotStoreFactory fileSnapshotStoreFactory = buildFileSnapshotStoreFactory(getIronSpiAwsInttestFilePath(), null);
        TransactionStoreFactory awsKinesisTransactionStoreFactory = buildAwsKinesisTransactionStoreFactory(m_configuration);

        try {
            TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
            SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
            SpiTest.checkThatCommandIsExecutedFromSnapshotStoreNotFromTransactionStore(awsKinesisTransactionStoreFactory, transactionSerializer, //
                                                                                       fileSnapshotStoreFactory, snapshotSerializer, //
                                                                                       randomStoreName);
        } finally {
            deleteKinesisStream(randomStoreName);
        }
    }
}
