package io.axway.iron.spi.aws.s3;

import java.nio.file.Paths;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import io.axway.iron.core.spi.file.FileStoreFactory;
import io.axway.iron.sample.Sample;
import io.axway.iron.spi.aws.AwsProperties;
import io.axway.iron.spi.jackson.JacksonSerializer;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static io.axway.iron.spi.aws.AwsTestUtils.getValue;
import static io.axway.iron.spi.aws.kinesis.AwsKinesisTestUtils.createStreamAndWaitActivationWithRandomName;
import static io.axway.iron.spi.aws.s3.AwsS3TestUtils.buildTestAwsS3SnapshotStoreFactory;

public class AwsS3SnapshotIT implements AwsProperties {

    public static final String DISABLE_VERIFY_CERTIFICATE = getValue(DISABLE_VERIFY_CERTIFICATE_KEY, "true");

    @DataProvider(name = "stores")
    public Object[][] providesStores() {

        if (DISABLE_VERIFY_CERTIFICATE.toLowerCase().equals("true")) {
            System.setProperty(DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "");
        }

        AwsS3SnapshotStoreFactory awsS3SnapshotStoreFactory = buildTestAwsS3SnapshotStoreFactory();
        FileStoreFactory fileStoreFactory = new FileStoreFactory(Paths.get("iron"));

        return new Object[][]{ //
                {fileStoreFactory, fileStoreFactory}, //
                {fileStoreFactory, awsS3SnapshotStoreFactory}, //
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
