package io.axway.iron.spi.jackson;

import java.util.function.*;
import io.axway.iron.spi.serializer.SnapshotSerializer;

public class JacksonSnapshotSerializerBuilder implements Supplier<SnapshotSerializer> {
    @Override
    public SnapshotSerializer get() {
        return new JacksonSerializer();
    }
}
