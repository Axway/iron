package io.axway.iron.core.bugs;

import java.util.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import io.axway.iron.Store;
import io.axway.iron.StoreManager;

import static org.assertj.core.api.Assertions.assertThat;

public class CommandWithLongCollectionParameterTest {
    @DataProvider(name = "longs")
    public Object[][] providesLongs() {
        return new Object[][]{ //
                new Object[]{Collections.emptyList(), null}, //
                new Object[]{Collections.singleton(null), null}, //
                new Object[]{Collections.singleton(42L), 42L}, //
                new Object[]{Arrays.asList(null, null), null}, //
                new Object[]{Arrays.asList(null, 15L, null), 15L}, //
                new Object[]{Arrays.asList(1L, 15L, 10L), 26L}, //
        };
    }

    @Test(dataProvider = "longs")
    public void ensureLongCollectionParameterInCommandWorks(Collection<Long> longParameters, Long expectedResult) throws Exception {
        try (StoreManager storeManager = IronTestHelper.createTransientStore()) {
            Store store = IronTestHelper.getRandomTransientStore(storeManager);

            Long result = store.createCommand(CommandWithLongCollectionParameterCommand.class) //
                    .set(CommandWithLongCollectionParameterCommand::longParameters).to(longParameters) //
                    .submit().get();

            assertThat(result).isEqualTo(expectedResult);
        }
    }
}
