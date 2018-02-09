package io.axway.iron.spi.aws.kinesis;

import java.nio.file.Paths;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import io.axway.iron.core.spi.file.FileStoreFactory;
import io.axway.iron.sample.Sample;
import io.axway.iron.spi.aws.AwsProperties;
import io.axway.iron.spi.jackson.JacksonSerializer;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static io.axway.iron.spi.aws.kinesis.AwsKinesisTestUtils.*;

public class AwsKinesisTransactionIT implements AwsProperties, AwsKinesisProperties {

    @DataProvider(name = "stores")
    public Object[][] providesStores() {
        if (DISABLE_VERIFY_CERTIFICATE.toLowerCase().equals("true")) {
            System.setProperty(DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "");
        }
        if (DISABLE_CBOR.toLowerCase().equals("true")) {
            System.setProperty(DISABLE_CBOR_SYSTEM_PROPERTY, "");
        }
        FileStoreFactory fileStoreFactory = new FileStoreFactory(Paths.get("iron"), null);
        KinesisTransactionStoreFactory kinesisTransactionStoreFactory = buildTestAwsKinesisTransactionStoreFactory();

        return new Object[][]{ //
                {fileStoreFactory, fileStoreFactory}, //
                {kinesisTransactionStoreFactory, fileStoreFactory}, //
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
    public void shouldRetrieveCommandsFromSnapshotStoreAndNotFromTransactionStore(TransactionStoreFactory transactionStoreFactory,
                                                                                  SnapshotStoreFactory snapshotStoreFactory) throws Exception {
        String storeName = AwsKinesisTestUtils.createStreamAndWaitActivationWithRandomName();
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        Sample.checkThatCommandIsExecutedFromSnapshotStoreNotFromTransactionStore(transactionStoreFactory, jacksonSerializer, snapshotStoreFactory,
                                                                                  jacksonSerializer, storeName);
    }
}
