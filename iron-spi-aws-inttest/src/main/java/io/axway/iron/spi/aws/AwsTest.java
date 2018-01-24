package io.axway.iron.spi.aws;

import java.util.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import io.axway.iron.sample.Sample;
import io.axway.iron.spi.jackson.JacksonSerializer;
import io.axway.iron.spi.kinesis.KinesisTransactionStoreFactory;
import io.axway.iron.spi.s3.AmazonS3SnapshotStoreFactory;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static io.axway.iron.spi.kinesis.AwsKinesisTestUtils.buildTestAwsKinesisTransactionStoreFactory;
import static io.axway.iron.spi.s3.AwsS3TestUtils.buildTestAwsS3SnapshotStoreFactory;

public class AwsTest {

    @DataProvider(name = "stores")
    public Object[][] providesStores() {

        KinesisTransactionStoreFactory awsKinesisTransactionStoreFactory = buildTestAwsKinesisTransactionStoreFactory();

        AmazonS3SnapshotStoreFactory awsS3SnapshotStoreFactory = buildTestAwsS3SnapshotStoreFactory();

        String storeBaseName = "irontest-" + System.getProperty("user.name");

        return new Object[][]{ //
                {awsKinesisTransactionStoreFactory, awsS3SnapshotStoreFactory, storeBaseName + "-" + UUID.randomUUID()}, //
        };
    }

    @Test(dataProvider = "stores")
    public void test(TransactionStoreFactory transactionStoreFactory, SnapshotStoreFactory snapshotStoreFactory, String name) throws Exception {
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        Sample.testCreateCompany(transactionStoreFactory, jacksonSerializer, snapshotStoreFactory, jacksonSerializer, name);
    }
}
