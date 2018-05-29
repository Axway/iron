package io.axway.iron.core.spi.testing;

import java.util.function.*;
import io.axway.iron.spi.storage.TransactionStore;

public class TransientTransactionStoreBuilder implements Supplier<TransactionStore> {
    @Override
    public TransactionStore get() {
        return new TransientStore();
    }
}
