package io.axway.iron.core.store;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import org.assertj.core.api.Assertions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import io.axway.iron.Store;
import io.axway.iron.StoreManager;
import io.axway.iron.core.StoreManagerBuilder;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

import static io.axway.iron.core.bugs.IronTestHelper.*;

public abstract class AbstractStoreTests {
    private static final String EXECUTION_ID;

    static {
        EXECUTION_ID = UUID.randomUUID().toString();
        System.out.printf("Execution ID=%s%n", EXECUTION_ID);
    }

    private final List<SucceedingStoreTest> m_succeedingStoreTestsStoreTests = new ArrayList<>();
    private final List<FailingStoreTest> m_failingStoreTests = new ArrayList<>();

    protected AbstractStoreTests(StoreTest... storeTests) {
        for (StoreTest storeTest : storeTests) {
            if (storeTest instanceof SucceedingStoreTest) {
                m_succeedingStoreTestsStoreTests.add((SucceedingStoreTest) storeTest);
            } else if (storeTest instanceof FailingStoreTest) {
                m_failingStoreTests.add((FailingStoreTest) storeTest);
            } else {
                throw new IllegalArgumentException("Test " + storeTest.getClass().getName() + " is not a SucceedingStoreTest or a FailingStoreTest");
            }
        }
    }

    @DataProvider(name = "succeedingStoreTests")
    public Object[][] providesSucceedingStoreTests() {
        Object[][] result = new Object[m_succeedingStoreTestsStoreTests.size()][];
        for (int i = 0; i < m_succeedingStoreTestsStoreTests.size(); i++) {
            result[i] = new Object[]{m_succeedingStoreTestsStoreTests.get(i)};
        }
        return result;
    }

    @DataProvider(name = "failingStoreTests")
    public Object[][] providesFailingStoreTests() {
        Object[][] result = new Object[m_failingStoreTests.size()][];
        for (int i = 0; i < m_failingStoreTests.size(); i++) {
            result[i] = new Object[]{m_failingStoreTests.get(i)};
        }
        return result;
    }

    @Test(dataProvider = "succeedingStoreTests", timeOut = 30_000)
    public void succeedingStoreTests(SucceedingStoreTest storeTest) throws Exception {
        String storeNamePrefix = storeTest.getClass().getSimpleName() + "-";

        // provision, execute and check in the same store execution, no final snapshot
        try (StoreManager storeManager = createStoreManager(storeTest)) {
            Store store = storeManager.getStore(storeNamePrefix + "1");
            storeTest.provision(store);
            storeTest.execute(store);
            store.query(storeTest::verify);
        }

        // check from a store recovered from tx logs
        try (StoreManager storeManager = createStoreManager(storeTest)) {
            Store store = storeManager.getStore(storeNamePrefix + "1");
            store.query(storeTest::verify);
        }

        // execute and check in the same store execution, with final snapshot
        try (StoreManager storeManager = createStoreManager(storeTest)) {
            Store store = storeManager.getStore(storeNamePrefix + "2");
            storeTest.provision(store);
            storeTest.execute(store);
            store.query(storeTest::verify);
            storeManager.snapshot();
        }

        // check from a store recovered from snapshot
        try (StoreManager storeManager = createStoreManager(storeTest)) {
            Store store = storeManager.getStore(storeNamePrefix + "2");
            store.query(storeTest::verify);
        }
    }

    @Test(dataProvider = "failingStoreTests", timeOut = 30_000)
    public void failingStoreTests(FailingStoreTest storeTest) throws Exception {
        String storeName = storeTest.getClass().getSimpleName();

        // execute and check in the same store execution, no final snapshot
        try (StoreManager storeManager = createStoreManager(storeTest)) {
            Store store = storeManager.getStore(storeName);
            storeTest.provision(store);
            try {
                storeTest.execute(store);
                Assertions.fail("Test %s didn't failed", storeTest.getClass().getName());
            } catch (Exception e) {
                System.out.println(String.format("Test %s failed as expected, with message: %s", store.getClass().getName(), e.getMessage()));
            }

            //TODO check that store has not been modified
        }
    }

    private StoreManager createStoreManager(StoreTest storeTest) throws Exception {
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();

        Path filePath = Paths.get("tmp-iron-test", "iron-core", "iron_tests-" + EXECUTION_ID);
        SnapshotStore snapshotStore = buildFileSnapshotStoreFactory(filePath, storeTest.getClass().getSimpleName());
        TransactionStore transactionStore = buildFileTransactionStoreFactory(filePath, storeTest.getClass().getSimpleName());

        StoreManagerBuilder builder = StoreManagerBuilder.newStoreManagerBuilder() //
                .withSnapshotSerializer(snapshotSerializer) //
                .withTransactionSerializer(transactionSerializer) //
                .withSnapshotStore(snapshotStore) //
                .withTransactionStore(transactionStore);
        storeTest.configure(builder);
        return builder.build();
    }
}
