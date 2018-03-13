package io.axway.iron.spi.file;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import org.testng.annotations.Test;
import io.axway.iron.spi.SpiTest;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static io.axway.iron.spi.file.FileTestHelper.*;
import static io.axway.iron.spi.jackson.JacksonTestHelper.*;

public class FileStoreTest {

    @Test
    public void shouldCreateCompanySequenceBeRight() throws Exception {
        String randomStoreName = "iron-store-" + UUID.randomUUID();
        Path filePath = Paths.get("iron", "iron-spi-file-inttest");

        TransactionStoreFactory transactionStoreFactory = buildFileTransactionStoreFactory(filePath, null);
        SnapshotStoreFactory snapshotStoreFactory = buildFileSnapshotStoreFactory(filePath, null);

        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();

        SpiTest.checkThatCreateCompanySequenceIsRight(transactionStoreFactory, transactionSerializer, snapshotStoreFactory, snapshotSerializer,
                                                      randomStoreName);
    }
}


