package io.axway.iron.core.bugs;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import io.axway.iron.Store;
import io.axway.iron.StoreManager;

import static org.assertj.core.api.Assertions.assertThat;

public class CommandWithLongParameterTest {
    @DataProvider(name = "longs")
    public Object[][] providesLongs() {
        return new Object[][]{ //
                new Object[]{null}, //
                new Object[]{42L}, //
        };
    }

    @Test(dataProvider = "longs")
    public void ensureLongParameterInCommandWorks(Long value) throws Exception {
        try (StoreManager storeManager = IronTestHelper.createTransientStore()) {
            Store store = IronTestHelper.getRandomTransientStore(storeManager);

            Long result = store.createCommand(CommandWithLongParameterCommand.class) //
                    .set(CommandWithLongParameterCommand::longParameter).to(value) //
                    .submit().get();

            assertThat(result).isEqualTo(value);
        }
    }
}
