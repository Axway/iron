package io.axway.iron.core.store;

import io.axway.iron.ReadOnlyTransaction;

public interface SucceedingStoreTest extends StoreTest {
    void verify(ReadOnlyTransaction tx);
}
