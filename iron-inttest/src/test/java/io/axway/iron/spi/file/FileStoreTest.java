package io.axway.iron.spi.file;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.*;
import org.testng.annotations.Test;
import io.axway.iron.spi.SpiTestHelper;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

import static io.axway.iron.spi.file.FileTestHelper.*;
import static io.axway.iron.spi.jackson.JacksonTestHelper.*;

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
}


