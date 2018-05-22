package io.axway.iron.core.bugs;

import java.util.concurrent.*;
import org.testng.annotations.Test;
import io.axway.iron.Store;
import io.axway.iron.StoreManager;
import io.axway.iron.core.model.simple.SimpleCommand;

/**
 * In case of a single command execution, a transaction is handled behind the hood. However if the transaction future is not strongly referenced from
 * the command future, it can lead to uncompleted transaction future and so uncompleted command future.
 * <p>
 * NB: this test is a best effort to reproduce the problem, depending on race condition it may happen than the test is not failing even if the transaction
 * future is not strongly referenced.
 */
public class UncompletableCommandFutureTest {

    @Test(timeOut = 20000)
    public void ensureCommandFutureComplete() throws Exception {
        try (StoreManager storeManager = IronTestHelper.createTransientStore()) {
            Store store = IronTestHelper.getRandomTransientStore(storeManager);

            Future<Void> future = store.createCommand(SimpleCommand.class).submit();
            System.gc();
            future.get();
        }
    }
}
