package io.axway.iron.spi.aws.kinesis;

import java.nio.file.Paths;
import org.testng.annotations.Test;
import io.axway.iron.core.spi.file.FileStoreFactory;
import io.axway.iron.sample.Sample;
import io.axway.iron.spi.aws.BaseInttest;
import io.axway.iron.spi.jackson.JacksonSerializer;

/**
 * Test KinesisTransactionStore and FileSnapshotStore
 */
public class AwsKinesisTransactionStoreIT extends BaseInttest {

    @Test
    public void shouldCreateCompanySequenceBeRight() throws Exception {
        String randomStoreName = createRandomStoreName();
        createStreamAndWaitActivation(randomStoreName);
        FileStoreFactory fileStoreFactory = new FileStoreFactory(Paths.get("iron"), null);
        KinesisTransactionStoreFactory kinesisTransactionStoreFactory = new KinesisTransactionStoreFactory(m_configuration);

        try {
            JacksonSerializer jacksonSerializer = new JacksonSerializer();
            Sample.checkThatCreateCompanySequenceIsRight(kinesisTransactionStoreFactory, jacksonSerializer, fileStoreFactory, jacksonSerializer,
                                                         randomStoreName);
        } finally {
            deleteKinesisStream(randomStoreName);
        }
    }

    @Test
    public void shouldRetrieveCommandsFromSnapshotStoreAndNotFromTransactionStoreSample() throws Exception {
        String randomStoreName = createRandomStoreName();
        FileStoreFactory fileStoreFactory = new FileStoreFactory(Paths.get("iron"), null);

        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        Sample.checkThatCommandIsExecutedFromSnapshotStoreNotFromTransactionStore(fileStoreFactory, jacksonSerializer, fileStoreFactory, jacksonSerializer,
                                                                                  randomStoreName);
    }

    @Test
    public void shouldRetrieveCommandsFromSnapshotStoreAndNot() throws Exception {
        String randomStoreName = createRandomStoreName();
        createStreamAndWaitActivation(randomStoreName);
        FileStoreFactory fileStoreFactory = new FileStoreFactory(Paths.get("iron"), null);
        KinesisTransactionStoreFactory kinesisTransactionStoreFactory = new KinesisTransactionStoreFactory(m_configuration);

        try {
            JacksonSerializer jacksonSerializer = new JacksonSerializer();
            Sample.checkThatCommandIsExecutedFromSnapshotStoreNotFromTransactionStore(kinesisTransactionStoreFactory, jacksonSerializer, fileStoreFactory,
                                                                                      jacksonSerializer, randomStoreName);
        } finally {
            deleteKinesisStream(randomStoreName);
        }
    }
}
