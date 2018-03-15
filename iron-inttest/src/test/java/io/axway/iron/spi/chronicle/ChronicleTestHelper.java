package io.axway.iron.spi.chronicle;

import java.nio.file.Path;
import io.axway.iron.spi.storage.TransactionStoreFactory;

public class ChronicleTestHelper {
    public static TransactionStoreFactory buildChronicleTransactionStoreFactory(Path filePath) {
        return new ChronicleTransactionStoreFactoryBuilder().setDir(filePath).get();
    }
}
