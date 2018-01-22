package io.axway.iron.spi.kinesis;

import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import io.axway.iron.core.spi.file.FileStoreFactory;
import io.axway.iron.sample.Sample;
import io.axway.iron.spi.jackson.JacksonSerializer;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStoreFactory;

public class KinesisTransactionTest {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd/mm/yyyy");

    @DataProvider(name = "stores")
    public Object[][] providesStores() {
        // Disable Cert checking to simplify testing (no need to manage certificates)
        System.setProperty("com.amazonaws.sdk.disableCertChecking", "");
        // Disable CBOR protocol which is not supported by kinesalite
        System.setProperty("com.amazonaws.sdk.disableCbor", "");

        FileStoreFactory fileStoreFactory = new FileStoreFactory(Paths.get("iron"));

        String accessKey = "AK";
        String secretKey = "SK";
        String region = "eu-west-1";
        String kinesisEndpoint = "localhost";
        Long kinesisPort = 4568L;
        String cloudwatchEndpoint = "localhost";
        Long cloudwatchPort = 4582L;
        // Disable certificate verification for testing purpose
        Boolean isVerifyCertificate = false;
        KinesisTransactionStoreFactory kinesisTransactionStoreFactory = new KinesisTransactionStoreFactory(accessKey, secretKey, region, kinesisEndpoint,
                                                                                                           kinesisPort, cloudwatchEndpoint, cloudwatchPort,
                                                                                                           isVerifyCertificate);

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
