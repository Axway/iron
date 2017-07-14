package io.axway.iron.core.store.id;

import io.axway.iron.core.store.AbstractStoreTests;

public class IdTests extends AbstractStoreTests {
    public IdTests() {
        super(new ShouldAutoGenerateIdTest());
    }
}
