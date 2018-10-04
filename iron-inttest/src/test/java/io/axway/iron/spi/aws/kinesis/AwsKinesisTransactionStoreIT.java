package io.axway.iron.spi.aws.kinesis;

import java.util.*;
import java.util.function.*;
import org.testng.annotations.Test;
import io.axway.iron.spi.SpiTestHelper;
import io.axway.iron.spi.aws.BaseInttest;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

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
        String randomFactoryName = "shouldCreateCompanySequenceBeRight-" + UUID.randomUUID();
        createStreamAndWaitActivation(randomFactoryName);
        Supplier<SnapshotStore> fileSnapshotStoreFactory = () -> buildFileSnapshotStore(getIronSpiAwsInttestFilePath(), randomFactoryName, null);
        Supplier<TransactionStore> awsKinesisTransactionStoreFactory = () -> buildAwsKinesisTransactionStoreFactory(randomFactoryName, m_configuration);

        try {
            TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
            SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
            SpiTestHelper.checkThatCreateCompanySequenceIsRight(awsKinesisTransactionStoreFactory, transactionSerializer,  //
                                                                fileSnapshotStoreFactory, snapshotSerializer,              //
                                                                randomStoreName);
        } finally {
            deleteKinesisStream(randomFactoryName);
        }
    }

    @Test(enabled = false)
    public void shouldRetrieveCommandsFromSnapshotFileStoreAndNotFromTransactionFileStore() throws Exception {
        String randomStoreName = createRandomStoreName();
        String randomFactoryName = "shouldRetrieveCommandsFromSnapshotFileStoreAndNotFromTransactionFileStore-" + UUID.randomUUID();
        Supplier<SnapshotStore> fileSnapshotStoreFactory = () -> buildFileSnapshotStore(getIronSpiAwsInttestFilePath(), randomFactoryName, null);
        Supplier<TransactionStore> fileTransactionStoreFactory = () -> buildFileTransactionStore(getIronSpiAwsInttestFilePath(), randomFactoryName);

        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
        SpiTestHelper.checkThatCommandIsExecutedFromSnapshotStoreNotFromTransactionStore(fileTransactionStoreFactory, transactionSerializer,         //
                                                                                         fileSnapshotStoreFactory, snapshotSerializer, //
                                                                                         randomStoreName);
    }

    @Test(enabled = false)
    public void shouldRetrieveCommandsFromSnapshotFileStoreAndNotFromTransactionKinesisStoreSample() throws Exception {
        String randomStoreName = createRandomStoreName();
        String randomFactoryName = UUID.randomUUID().toString();

        createStreamAndWaitActivation(randomFactoryName);
        Supplier<SnapshotStore> fileSnapshotStoreFactory = () -> buildFileSnapshotStore(getIronSpiAwsInttestFilePath(), randomFactoryName, null);
        Supplier<TransactionStore> awsKinesisTransactionStoreFactory = () -> buildAwsKinesisTransactionStoreFactory(randomFactoryName, m_configuration);

        try {
            TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
            SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
            SpiTestHelper.checkThatCommandIsExecutedFromSnapshotStoreNotFromTransactionStore(awsKinesisTransactionStoreFactory, transactionSerializer, //
                                                                                             fileSnapshotStoreFactory, snapshotSerializer, //
                                                                                             randomStoreName);
        } finally {
            deleteKinesisStream(randomFactoryName);
        }
    }
}
