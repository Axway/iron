package io.axway.iron.spi.kinesis;

import java.nio.file.Paths;
import java.util.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import io.axway.iron.core.spi.file.FileStoreFactory;
import io.axway.iron.sample.Sample;
import io.axway.iron.spi.jackson.JacksonSerializer;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static io.axway.iron.spi.kinesis.AwsKinesisTestUtils.buildTestAwsKinesisTransactionStoreFactory;

public class AwsKinesisTransactionTest {

    @DataProvider(name = "stores")
    public Object[][] providesStores() {
        FileStoreFactory fileStoreFactory = new FileStoreFactory(Paths.get("iron"));

        KinesisTransactionStoreFactory kinesisTransactionStoreFactory = buildTestAwsKinesisTransactionStoreFactory();

        String storeBaseName = "irontest-" + System.getProperty("user.name");

        return new Object[][]{ //
                {kinesisTransactionStoreFactory, fileStoreFactory, storeBaseName + "-" + UUID.randomUUID()}, //
        };
    }

    @Test(dataProvider = "stores")
    public void test(TransactionStoreFactory transactionStoreFactory, SnapshotStoreFactory snapshotStoreFactory, String name) throws Exception {
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        Sample.testCreateCompany(transactionStoreFactory, jacksonSerializer, snapshotStoreFactory, jacksonSerializer, name);
    }
}
