package io.axway.iron.core.store;

import io.axway.iron.Store;
import io.axway.iron.core.StoreManagerFactoryBuilder;

public interface StoreTest {
    void configure(StoreManagerFactoryBuilder builder) throws Exception;

    default void provision(Store store) throws Exception {
        // by default, do nothing
    }

    void execute(Store store) throws Exception;
}
