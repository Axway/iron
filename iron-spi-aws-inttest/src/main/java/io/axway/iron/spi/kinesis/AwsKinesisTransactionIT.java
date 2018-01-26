package io.axway.iron.spi.kinesis;

import java.nio.file.Paths;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import io.axway.iron.core.spi.file.FileStoreFactory;
import io.axway.iron.sample.Sample;
import io.axway.iron.spi.jackson.JacksonSerializer;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static io.axway.iron.spi.kinesis.AwsKinesisTestUtils.*;

public class AwsKinesisTransactionIT {

    @DataProvider(name = "stores")
    public Object[][] providesStores() {
        setSystemPropertyForDev();

        FileStoreFactory fileStoreFactory = new FileStoreFactory(Paths.get("iron"));
        KinesisTransactionStoreFactory kinesisTransactionStoreFactory = buildTestAwsKinesisTransactionStoreFactory();

        return new Object[][]{ //
                {fileStoreFactory, fileStoreFactory}, //
                {kinesisTransactionStoreFactory, fileStoreFactory}, //
        };
    }

    @Test(dataProvider = "stores")
    public void test(TransactionStoreFactory transactionStoreFactory, SnapshotStoreFactory snapshotStoreFactory) throws Exception {
        String storeName = createStreamAndWaitActivationWithRandomName();
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        Sample.testCreateCompany(transactionStoreFactory, jacksonSerializer, snapshotStoreFactory, jacksonSerializer, storeName);
    }

    @Test(dataProvider = "stores")
    public void testStore(TransactionStoreFactory transactionStoreFactory, SnapshotStoreFactory snapshotStoreFactory) throws Exception {
        String storeName = createStreamAndWaitActivationWithRandomName();
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        Sample.testStore(transactionStoreFactory, jacksonSerializer, snapshotStoreFactory, jacksonSerializer, storeName);
    }
}
