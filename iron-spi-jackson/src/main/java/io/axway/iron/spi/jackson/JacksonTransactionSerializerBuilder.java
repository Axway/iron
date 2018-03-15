package io.axway.iron.spi.jackson;

import java.util.function.*;
import io.axway.iron.spi.serializer.TransactionSerializer;

public class JacksonTransactionSerializerBuilder implements Supplier<TransactionSerializer> {
    @Override
    public TransactionSerializer get() {
        return new JacksonSerializer();
    }
}
