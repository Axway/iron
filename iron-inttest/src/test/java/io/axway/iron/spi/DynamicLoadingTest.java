package io.axway.iron.spi;

import java.util.*;
import org.assertj.core.api.Assertions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import io.axway.iron.core.FakeStoreManagerFactoryBuilderImpl;
import io.axway.iron.core.internal.StoreManagerFactoryBuilderConfigurator;
import io.axway.iron.spi.aws.AwsTestHelper;

import static io.axway.iron.spi.aws.BaseInttest.loadConfiguration;

@Test
public class DynamicLoadingTest {
    private static final String SNAPSHOT_STORE_FACTORY = "SnapshotStoreFactory".toLowerCase();
    private static final String TRANSACTION_STORE_FACTORY = "TransactionStoreFactory".toLowerCase();
    private static final String SNAPSHOT_SERIALIZER = "SnapshotSerializer".toLowerCase();
    private static final String TRANSACTION_SERIALIZER = "TransactionSerializer".toLowerCase();

    private static final String TEST_IMPLEMENTATIONS_KEY = "test.implementations";

    private final StoreManagerFactoryBuilderConfigurator m_configurator = new StoreManagerFactoryBuilderConfigurator();

    @DataProvider
    public Object[] getConfigurationFilenames() {
        return new String[]{ //
                "kinesis_configuration.properties", //
                "s3_configuration.properties", //
                "files_configuration.properties", //
                "kafka_configuration.properties", //
                "jackson_configuration.properties", //
                "chronicle_configuration.properties", //
        };
    }

    @Test(dataProvider = "getConfigurationFilenames", enabled = false)
    public void shouldConfigureAndSetTheRightsImplementations(String configurationFilename) {
        Properties configuration = loadConfiguration(configurationFilename);
        if (configuration.size() == 0) {
            throw new IllegalArgumentException("Please use a valid property file");
        }
        AwsTestHelper.handleLocalStackConfigurationForLocalTesting(configuration); // needed for AWS localstack tests
        FakeStoreManagerFactoryBuilderImpl factoryBuilder = new FakeStoreManagerFactoryBuilderImpl();

        m_configurator.fill(factoryBuilder, configuration);

        checkConfigurator(factoryBuilder, configuration.getProperty(TEST_IMPLEMENTATIONS_KEY));
    }

    private void checkConfigurator(FakeStoreManagerFactoryBuilderImpl factoryBuilder, String property) {
        String[] implementations = property.split(",");
        for (String implementation : implementations) {
            String imp = implementation.toLowerCase();
            if (imp.equals(SNAPSHOT_STORE_FACTORY)) {
                Assertions.assertThat(factoryBuilder.m_snapshotStoreFactory).isNotNull();
            } else if (imp.equals(TRANSACTION_STORE_FACTORY)) {
                Assertions.assertThat(factoryBuilder.m_transactionStoreFactory).isNotNull();
            } else if (imp.equals(SNAPSHOT_SERIALIZER)) {
                Assertions.assertThat(factoryBuilder.m_snapshotSerializer).isNotNull();
            } else if (imp.equals(TRANSACTION_SERIALIZER)) {
                Assertions.assertThat(factoryBuilder.m_transactionSerializer).isNotNull();
            }
        }
    }
}
