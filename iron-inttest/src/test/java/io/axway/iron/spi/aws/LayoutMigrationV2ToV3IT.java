package io.axway.iron.spi.aws;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.*;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import io.axway.iron.Store;
import io.axway.iron.StoreManager;
import io.axway.iron.core.StoreManagerBuilder;
import io.axway.iron.sample.command.CreateCar;
import io.axway.iron.sample.command.CreateCompany;
import io.axway.iron.sample.command.CreatePerson;
import io.axway.iron.sample.model.Car;
import io.axway.iron.sample.model.Company;
import io.axway.iron.sample.model.Person;
import io.axway.iron.spi.aws.s3.LayoutMigrationV2ToV3;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;

import static io.axway.iron.spi.aws.AwsTestHelper.*;
import static io.axway.iron.spi.file.FileTestHelper.*;
import static io.axway.iron.spi.jackson.JacksonTestHelper.*;
import static org.assertj.core.api.Assertions.assertThat;

public class LayoutMigrationV2ToV3IT extends BaseInttest {

    private final Properties m_configuration = loadConfiguration("configuration.properties");
    private String m_bucketName;

    private SnapshotSerializer m_snapshotSerializer = buildJacksonSnapshotSerializer();
    private TransactionSerializer m_transactionSerializer = buildJacksonTransactionSerializer();

    private Path m_storesFilePath;

    @BeforeMethod
    public void createBucketAndStream() {
        m_bucketName = createRandomBucketName();
        String directoryName = createRandomDirectoryName();
        m_storesFilePath = Paths.get(directoryName);
        m_configuration.setProperty(S3_DIRECTORY_NAME, directoryName);
        m_configuration.setProperty(S3_BUCKET_NAME, m_bucketName);
        createS3Bucket(m_bucketName);
    }

    @AfterClass
    public void afterMethod() throws IOException {
        if (m_storesFilePath != null && m_storesFilePath.toFile().exists()) {
            try (Stream<Path> files = Files.walk(m_storesFilePath)) {
                for (Path path : files.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        System.out.println("Cannot delete " + path + " due to " + e);
                    }
                }
            }
        }
        deleteS3Bucket(m_bucketName);
    }

    @Test(enabled = false)
    public void shouldStartFromAStoreMigratedFromFileV2ToAwsV3() throws Exception {
        String companyStoreName = "companies";
        String carStoreName = "cars";
        try {
            // Given two "SPI File" stores ("companies" and "cars") filled with data
            try (StoreManager storeManager = StoreManagerBuilder.newStoreManagerBuilder() //
                    .withTransactionSerializer(m_transactionSerializer) //
                    .withTransactionStore(buildFileTransactionStore(m_storesFilePath, companyStoreName)) //
                    .withSnapshotSerializer(m_snapshotSerializer) //
                    .withSnapshotStore(buildFileSnapshotStore(m_storesFilePath, companyStoreName)) //
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
            }

            try (StoreManager storeManager = StoreManagerBuilder.newStoreManagerBuilder() //
                    .withTransactionSerializer(m_transactionSerializer) //
                    .withTransactionStore(buildFileTransactionStore(m_storesFilePath, carStoreName)) //
                    .withSnapshotSerializer(m_snapshotSerializer) //
                    .withSnapshotStore(buildFileSnapshotStore(m_storesFilePath, carStoreName)) //
                    .withEntityClass(Car.class) //
                    .withCommandClass(CreateCar.class) //
                    .build()) {

                storeManager.getStore("storeCarA").
                        createCommand(CreateCar.class).
                        set(CreateCar::name).to("Aston-Martin").
                        submit().get();
                storeManager.snapshot();
                storeManager.getStore("storeCarB").
                        createCommand(CreateCar.class).
                        set(CreateCar::name).to("Buick").
                        submit().get();
                storeManager.snapshot();
                storeManager.getStore("storeCarC").
                        createCommand(CreateCar.class).
                        set(CreateCar::name).to("Cadillac").
                        submit().get();
                storeManager.getStore("storeCarC").
                        createCommand(CreateCar.class).
                        set(CreateCar::name).to("Continental").
                        submit().get();
                storeManager.getStore("storeCarC").
                        createCommand(CreateCar.class).
                        set(CreateCar::name).to("Corvette").
                        submit().get();
                storeManager.snapshot();
            }

            // When I migrate the snapshots to "SPI S3"
            String endpoint = m_configuration.getProperty(S3_ENDPOINT);
            String port = m_configuration.getProperty(S3_PORT);
            String region = m_configuration.getProperty(S3_REGION);
            LayoutMigrationV2ToV3
                    .main(new String[]{region, m_storesFilePath.toFile().getAbsolutePath(), m_bucketName, m_configuration.getProperty(S3_DIRECTORY_NAME),
                            endpoint, port});

            createStreamAndWaitActivation(carStoreName);
            createStreamAndWaitActivation(companyStoreName);

            try (StoreManager storeManager = StoreManagerBuilder.newStoreManagerBuilder() //
                    .withTransactionSerializer(m_transactionSerializer) //
                    .withTransactionStore(buildAwsKinesisTransactionStoreFactory(companyStoreName, m_configuration)) //
                    .withSnapshotSerializer(m_snapshotSerializer) //
                    .withSnapshotStore(buildAwsS3SnapshotStoreFactory(companyStoreName, m_configuration)) //
                    .withEntityClass(Company.class) //
                    .withEntityClass(Person.class) //
                    .withCommandClass(CreateCompany.class) //
                    .withCommandClass(CreatePerson.class) //
                    .build()) {

                assertThat(storeManager.listStores()).hasSize(2);
                Store store = storeManager.getStore("storeCompanyB");
                assertThat(store.query(tx -> {
                    return tx.select(Company.class).all();
                }).stream().map(Company::name)).containsExactly("Bernard", "Bianca");
            }

            try (StoreManager storeManager = StoreManagerBuilder.newStoreManagerBuilder() //
                    .withTransactionSerializer(m_transactionSerializer) //
                    .withTransactionStore(buildAwsKinesisTransactionStoreFactory(carStoreName, m_configuration)) //
                    .withSnapshotSerializer(m_snapshotSerializer) //
                    .withSnapshotStore(buildAwsS3SnapshotStoreFactory(carStoreName, m_configuration)) //
                    .withEntityClass(Car.class) //
                    .withCommandClass(CreateCar.class) //
                    .build()) {

                assertThat(storeManager.listStores()).hasSize(3);
                Store store = storeManager.getStore("storeCarC");
                assertThat(store.query(tx -> {
                    return tx.select(Car.class).all();
                }).stream().map(Car::name)).containsExactly("Cadillac", "Continental", "Corvette");
            }
        } finally {
            deleteKinesisStream(companyStoreName);
            deleteKinesisStream(carStoreName);
        }
    }
}
