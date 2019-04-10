package io.axway.iron.spi.aws;

import java.util.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.axway.iron.Store;
import io.axway.iron.StoreManager;
import io.axway.iron.core.StoreManagerBuilder;
import io.axway.iron.sample.command.CreateCompany;
import io.axway.iron.sample.command.CreatePerson;
import io.axway.iron.sample.model.Company;
import io.axway.iron.sample.model.Person;
import io.axway.iron.spi.SpiTestHelper;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;

import static io.axway.iron.spi.aws.AwsTestHelper.*;
import static io.axway.iron.spi.jackson.JacksonTestHelper.*;
import static org.assertj.core.api.Assertions.assertThat;

public class AwsKinesisTransactionStoreS3SnapshotStoreIT extends BaseInttest {

    private String m_bucketName;
    private String m_storeName;

    private SnapshotSerializer m_snapshotSerializer = buildJacksonSnapshotSerializer();
    private TransactionSerializer m_transactionSerializer = buildJacksonTransactionSerializer();

    @BeforeMethod
    public void createBucketAndStream() {
        m_bucketName = createRandomBucketName();
        m_storeName = createRandomStoreName();
        String directoryName = createRandomDirectoryName();
        m_configuration.setProperty(S3_DIRECTORY_NAME, directoryName);
        m_configuration.setProperty(S3_BUCKET_NAME, m_bucketName);
        createStreamAndWaitActivation(m_storeName);
        createS3Bucket(m_bucketName);
    }

    @AfterMethod
    public void deleteBucketAndStream() {
        deleteKinesisStream(m_storeName);
        deleteS3Bucket(m_bucketName);
    }

    @Test(enabled = false)
    public void shouldCreateCompanySequenceBeRight() throws Exception {

        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();

        String factoryName = "shouldCreateCompanySequenceBeRight-" + UUID.randomUUID();
        SpiTestHelper.checkThatCreateCompanySequenceIsRight(() -> buildAwsKinesisTransactionStoreFactory(factoryName, m_configuration), transactionSerializer,
                                                            () -> buildAwsS3SnapshotStoreFactory(factoryName, m_configuration), snapshotSerializer,
                                                            m_storeName);
    }

    @Test(enabled = false)
    public void shouldListSnapshotsReturnTheRightNumberOfSnapshots() throws Exception {
        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
        String factoryName = "shouldListSnapshotsReturnTheRightNumberOfSnapshots-" + UUID.randomUUID();

        SpiTestHelper.checkThatListSnapshotsReturnTheRightNumberOfSnapshots(buildAwsKinesisTransactionStoreFactory(factoryName, m_configuration),
                                                                            transactionSerializer, buildAwsS3SnapshotStoreFactory(factoryName, m_configuration),
                                                                            snapshotSerializer, m_storeName);
    }

    @Test(enabled = false)
    public void shouldRetrieveCommandsFromSnapshotStoreAndNotFromTransactionStore() throws Exception {
        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
        String factoryName = "shouldRetrieveCommandsFromSnapshotStoreAndNotFromTransactionStore-" + UUID.randomUUID();

        SpiTestHelper
                .checkThatCommandIsExecutedFromSnapshotStoreNotFromTransactionStore(() -> buildAwsKinesisTransactionStoreFactory(factoryName, m_configuration),
                                                                                    transactionSerializer,
                                                                                    () -> buildAwsS3SnapshotStoreFactory(factoryName, m_configuration),
                                                                                    snapshotSerializer, m_storeName);
    }

    @Test(enabled = false)
    public void shouldStartOnTheLastValidSnapshot() throws Exception {
        try {
            createStreamAndWaitActivation("companies_last_valid");

            // Given a storeManager with two stores storeCompanyA and storeCompanyB
            createsSpiFileStoreTwoCompanyStoresWithContent("companies_last_valid");

            // Remove I invalidate the second snapshot
            AmazonS3 s3Client = buildS3Client(m_configuration);
            String lastId = s3Client.listObjects(m_bucketName).getObjectSummaries().stream().reduce((first, second) -> second).map(S3ObjectSummary::getKey)
                    .get();
            String storeCompanyBSnapshot = lastId.replace("/ids/", "/data/") + "/storeCompanyB.snapshot";
            s3Client.deleteObject(m_bucketName, lastId);
            s3Client.deleteObject(m_bucketName, storeCompanyBSnapshot);

            // Then storeManager can open the store storeCompanyB
            // The store storeCompanyB contains
            try (StoreManager storeManager = StoreManagerBuilder.newStoreManagerBuilder() //
                    .withTransactionSerializer(m_transactionSerializer) //
                    .withTransactionStore(buildAwsKinesisTransactionStoreFactory("companies_last_valid", m_configuration)) //
                    .withSnapshotSerializer(m_snapshotSerializer) //
                    .withSnapshotStore(buildAwsS3SnapshotStoreFactory("companies_last_valid", m_configuration)) //
                    .withEntityClass(Company.class) //
                    .withEntityClass(Person.class) //
                    .withCommandClass(CreateCompany.class) //
                    .withCommandClass(CreatePerson.class) //
                    .build()) {

                assertThat(storeManager.listStores()).hasSize(2);
                Store storeCompanyA = storeManager.getStore("storeCompanyA");
                assertThat(storeCompanyA.query(tx -> {
                    return tx.select(Company.class).all();
                }).stream().map(Company::name)).containsExactly("Albert");
                Store storeCompanyB = storeManager.getStore("storeCompanyB");
                assertThat(storeCompanyB.query(tx -> {
                    return tx.select(Company.class).all();
                }).stream().map(Company::name)).containsExactly("Bernard", "Bianca");
            }
        } finally {
            deleteKinesisStream("companies_last_valid");
        }
    }

    @Test(enabled = false)
    public void shouldStartOnTheLastValidSnapshotWithInvalidCorruptedSnapshot() throws Exception {
        String storeName = "companies_corrupted";
        try {
            createStreamAndWaitActivation(storeName);

            // Creates a storeManager and add two stores storeCompanyA and storeCompanyB
            createsSpiFileStoreTwoCompanyStoresWithContent(storeName);

            // Remove the second snapshot id and corrupt the second snapshot (remove storeCompanyB.snapshot)
            AmazonS3 s3Client = buildS3Client(m_configuration);
            String lastId = s3Client.listObjects(m_bucketName).getObjectSummaries().stream().reduce((first, second) -> second).map(S3ObjectSummary::getKey)
                    .get();
            String storeCompanyBSnapshot = lastId.replace("/ids/", "/data/") + "/storeCompanyB.snapshot";
            s3Client.deleteObject(m_bucketName, storeCompanyBSnapshot);

            // Open the storeManager and check that the second snapshot is not taken into account
            try (StoreManager storeManager = StoreManagerBuilder.newStoreManagerBuilder() //
                    .withTransactionSerializer(m_transactionSerializer) //
                    .withTransactionStore(buildAwsKinesisTransactionStoreFactory(storeName, m_configuration)) //
                    .withSnapshotSerializer(m_snapshotSerializer) //
                    .withSnapshotStore(buildAwsS3SnapshotStoreFactory(storeName, m_configuration)) //
                    .withEntityClass(Company.class) //
                    .withEntityClass(Person.class) //
                    .withCommandClass(CreateCompany.class) //
                    .withCommandClass(CreatePerson.class) //
                    .build()) {

                assertThat(storeManager.listStores()).hasSize(1);
                Store storeCompanyA = storeManager.getStore("storeCompanyA");
                assertThat(storeCompanyA.query(tx -> {
                    return tx.select(Company.class).all();
                }).stream().map(Company::name)).containsExactly("Albert");
            }
        } finally {
            deleteKinesisStream(storeName);
        }
    }

    private void createsSpiFileStoreTwoCompanyStoresWithContent(String storeName) throws InterruptedException, java.util.concurrent.ExecutionException {
        try (StoreManager storeManager = StoreManagerBuilder.newStoreManagerBuilder() //
                .withTransactionSerializer(m_transactionSerializer) //
                .withTransactionStore(buildAwsKinesisTransactionStoreFactory(storeName, m_configuration)) //
                .withSnapshotSerializer(m_snapshotSerializer) //
                .withSnapshotStore(buildAwsS3SnapshotStoreFactory(storeName, m_configuration)) //
                .withEntityClass(Company.class) //
                .withEntityClass(Person.class) //
                .withCommandClass(CreateCompany.class) //
                .withCommandClass(CreatePerson.class) //
                .build()) {

            storeManager.getStore("storeCompanyA").
                    createCommand(CreateCompany.class).
                    set(CreateCompany::name).to("Albert").
                    submit().get();
            storeManager.snapshot();
            storeManager.getStore("storeCompanyB").
                    createCommand(CreateCompany.class).
                    set(CreateCompany::name).to("Bernard").
                    submit().get();
            storeManager.getStore("storeCompanyB").
                    createCommand(CreateCompany.class).
                    set(CreateCompany::name).to("Bianca").
                    submit().get();
            storeManager.snapshot();

            assertThat(storeManager.listStores()).hasSize(2);
            Store storeCompanyA = storeManager.getStore("storeCompanyA");
            assertThat(storeCompanyA.query(tx -> {
                return tx.select(Company.class).all();
            }).stream().map(Company::name)).containsExactly("Albert");
            Store storeCompanyB = storeManager.getStore("storeCompanyB");
            assertThat(storeCompanyB.query(tx -> {
                return tx.select(Company.class).all();
            }).stream().map(Company::name)).containsExactly("Bernard", "Bianca");
        }
    }
}
