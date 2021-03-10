package io.axway.iron.core.store;

import io.axway.iron.ReadonlyTransaction;

public interface SucceedingStoreTest extends StoreTest {
    void verify(ReadonlyTransaction tx);
}
