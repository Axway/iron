package io.axway.iron.spi.s3;

import java.nio.file.Paths;
import java.util.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import io.axway.iron.core.spi.file.FileStoreFactory;
import io.axway.iron.sample.Sample;
import io.axway.iron.spi.jackson.JacksonSerializer;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static io.axway.iron.spi.s3.AwsS3TestUtils.buildTestAwsS3SnapshotStoreFactory;

public class S3SnapshotTest {

    @DataProvider(name = "stores")
    public Object[][] providesStores() {
        AmazonS3SnapshotStoreFactory amazonS3SnapshotStoreFactory = buildTestAwsS3SnapshotStoreFactory();

        FileStoreFactory fileStoreFactory = new FileStoreFactory(Paths.get("iron"));

        String storeBaseName = "irontest-" + System.getProperty("user.name");

        return new Object[][]{ //
                {fileStoreFactory, amazonS3SnapshotStoreFactory, storeBaseName + "-" + UUID.randomUUID()}, //
        };
    }

    @Test(dataProvider = "stores")
    public void test(TransactionStoreFactory transactionStoreFactory, SnapshotStoreFactory snapshotStoreFactory, String name) throws Exception {
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        Sample.testCreateCompany(transactionStoreFactory, jacksonSerializer, snapshotStoreFactory, jacksonSerializer, name);
    }
}
