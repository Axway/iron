package io.axway.iron.spi.s3;

import java.nio.file.Paths;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import io.axway.iron.core.spi.file.FileStoreFactory;
import io.axway.iron.sample.Sample;
import io.axway.iron.spi.jackson.JacksonSerializer;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static io.axway.iron.spi.kinesis.AwsKinesisTestUtils.*;
import static io.axway.iron.spi.s3.AwsS3TestUtils.buildTestAwsS3SnapshotStoreFactory;

public class AwsS3SnapshotIT {

    @DataProvider(name = "stores")
    public Object[][] providesStores() {
        setSystemPropertyForLocalstackKinesis();

        AmazonS3SnapshotStoreFactory amazonS3SnapshotStoreFactory = buildTestAwsS3SnapshotStoreFactory();
        FileStoreFactory fileStoreFactory = new FileStoreFactory(Paths.get("iron"));

        return new Object[][]{ //
                {fileStoreFactory, fileStoreFactory}, //
                {fileStoreFactory, amazonS3SnapshotStoreFactory}, //
        };
    }

    @Test(dataProvider = "stores")
    public void shouldCreateCompanySequenceBeRight(TransactionStoreFactory transactionStoreFactory, SnapshotStoreFactory snapshotStoreFactory)
            throws Exception {
        String storeName = createStreamAndWaitActivationWithRandomName();
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        Sample.checkThatCreateCompanySequenceIsRight(transactionStoreFactory, jacksonSerializer, snapshotStoreFactory, jacksonSerializer, storeName);
    }

    @Test(dataProvider = "stores")
    public void shouldListSnapshotsReturnTheRightNumberOfSnapshots(TransactionStoreFactory transactionStoreFactory, SnapshotStoreFactory snapshotStoreFactory)
            throws Exception {
        String storeName = createStreamAndWaitActivationWithRandomName();
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        Sample.checkThatListSnapshotsReturnTheRightNumberOfSnapshots(transactionStoreFactory, jacksonSerializer, snapshotStoreFactory, jacksonSerializer,
                                                                     storeName);
    }
}
