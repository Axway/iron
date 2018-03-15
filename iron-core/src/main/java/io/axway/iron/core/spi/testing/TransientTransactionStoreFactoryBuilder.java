package io.axway.iron.core.spi.testing;

import java.util.function.*;
import io.axway.iron.spi.storage.TransactionStoreFactory;

public class TransientTransactionStoreFactoryBuilder implements Supplier<TransactionStoreFactory> {
    @Override
    public TransactionStoreFactory get() {
        return new TransientStoreFactory();
    }
}
