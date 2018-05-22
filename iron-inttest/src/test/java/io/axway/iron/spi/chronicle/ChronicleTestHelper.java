package io.axway.iron.spi.chronicle;

import java.nio.file.Path;
import io.axway.iron.spi.storage.TransactionStore;

public class ChronicleTestHelper {
    public static TransactionStore buildChronicleTransactionStoreFactory(String name, Path filePath) {
        return new ChronicleTransactionStoreBuilder(name).setDir(filePath).get();
    }
}
