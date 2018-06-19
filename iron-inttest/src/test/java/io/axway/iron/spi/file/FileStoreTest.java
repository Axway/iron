package io.axway.iron.spi.file;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.*;
import org.testng.annotations.Test;
import io.axway.iron.StoreManager;
import io.axway.iron.core.StoreManagerBuilder;
import io.axway.iron.sample.command.ChangeCompanyAddress;
import io.axway.iron.sample.command.CreateCompany;
import io.axway.iron.sample.command.CreatePerson;
import io.axway.iron.sample.command.DeleteCompany;
import io.axway.iron.sample.command.PersonJoinCompany;
import io.axway.iron.sample.command.PersonLeaveCompany;
import io.axway.iron.sample.command.PersonRaiseSalary;
import io.axway.iron.sample.model.Company;
import io.axway.iron.sample.model.Person;
import io.axway.iron.spi.SpiTestHelper;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

import static io.axway.iron.spi.file.FileTestHelper.*;
import static io.axway.iron.spi.jackson.JacksonTestHelper.*;
import static java.util.Comparator.*;
import static org.assertj.core.api.Assertions.assertThat;

public class FileStoreTest {

    @Test
    public void shouldCreateCompanySequenceBeRight() throws Exception {
        String randomStoreName = "iron-store-" + UUID.randomUUID();
        Path filePath = Paths.get("tmp-iron-test", "iron-spi-file-inttest");

        Supplier<TransactionStore> transactionStoreFactory = () -> buildFileTransactionStore(filePath, "shouldCreateCompanySequenceBeRight");
        Supplier<SnapshotStore> snapshotStoreFactory = () -> buildFileSnapshotStore(filePath, "shouldCreateCompanySequenceBeRight");

        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();

        SpiTestHelper.checkThatCreateCompanySequenceIsRight(transactionStoreFactory, transactionSerializer, snapshotStoreFactory, snapshotSerializer,
                                                            randomStoreName);
    }

    @Test
    public void shouldBeAbleToDeleteFilesAfterStoreManagerClose() throws IOException, InterruptedException {
        Path filePath = Paths.get("tmp-iron-test", "iron-spi-file-inttest");

        String directory = "iron-directory";
        Supplier<TransactionStore> transactionStoreFactory = () -> buildFileTransactionStore(filePath, directory);
        Supplier<SnapshotStore> snapshotStoreFactory = () -> buildFileSnapshotStore(filePath, directory);

        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();

        StoreManagerBuilder factoryBuilder = StoreManagerBuilder.newStoreManagerBuilder() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStore(transactionStoreFactory.get()) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStore(snapshotStoreFactory.get()) //
                .withEntityClass(Company.class) //
                .withEntityClass(Person.class) //
                .withCommandClass(ChangeCompanyAddress.class) //
                .withCommandClass(CreateCompany.class) //
                .withCommandClass(CreatePerson.class) //
                .withCommandClass(DeleteCompany.class) //
                .withCommandClass(PersonJoinCompany.class) //
                .withCommandClass(PersonLeaveCompany.class) //
                .withCommandClass(PersonRaiseSalary.class);

        try (StoreManager storeManager = factoryBuilder.build()) {
            storeManager.getStore("test");
            storeManager.snapshot();
        }

        assertThat(Files.walk(Paths.get("tmp-iron-test")).
                sorted(reverseOrder()).map(Path::toFile).
                map(file -> new Object[]{file, file.delete()})).
                allMatch(fileStatus -> (boolean) fileStatus[1]);
    }
}


