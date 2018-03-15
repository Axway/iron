package io.axway.iron.spi.jackson;

import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;

public class JacksonTestHelper {
    public static SnapshotSerializer buildJacksonSnapshotSerializer() {
        return new JacksonSnapshotSerializerBuilder().get();
    }

    public static TransactionSerializer buildJacksonTransactionSerializer() {
        return new JacksonTransactionSerializerBuilder().get();
    }
}
