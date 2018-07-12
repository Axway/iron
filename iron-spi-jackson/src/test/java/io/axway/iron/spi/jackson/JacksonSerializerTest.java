package io.axway.iron.spi.jackson;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import org.testng.annotations.Test;
import io.axway.iron.spi.model.snapshot.SerializableAttributeDefinition;
import io.axway.iron.spi.model.snapshot.SerializableEntity;
import io.axway.iron.spi.model.snapshot.SerializableInstance;
import io.axway.iron.spi.model.snapshot.SerializableSnapshot;

import static org.assertj.core.api.Assertions.assertThat;

public class JacksonSerializerTest {

    @Test
    public void shouldProvideTheStoreNameInTheDeSerializedSnapshot() throws IOException {
        // Given a jacksonSerializer and a resource io/axway/iron/spi/jackson/simple.snapshot.json
        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("io/axway/iron/spi/jackson/simple.snapshot.json");
        // When I deserialize a snapshot
        SerializableSnapshot serializableSnapshot = jacksonSerializer.deserializeSnapshot("myStoreName", inputStream);
        // Then the snapshot reflects the resource content, and contains also the storeName
        assertThat(serializableSnapshot.getSnapshotModelVersion()).isEqualTo(123L);
        assertThat(serializableSnapshot.getTransactionId()).isEqualTo(new BigInteger("123456789"));
        assertThat(serializableSnapshot.getEntities()).hasSize(1);
        SerializableEntity serializableEntity = serializableSnapshot.getEntities().stream().findFirst().get();
        assertThat(serializableEntity.getNextId()).isEqualTo(1L);
        assertThat(serializableEntity.getEntityName()).isEqualTo("io.axway.iron.spi.jackson.SimpleEntity");
        assertThat(serializableEntity.getRelations()).isEmpty();
        assertThat(serializableEntity.getAttributes()).containsOnlyKeys("simpleAttribute");
        SerializableAttributeDefinition simpleAttribute = serializableEntity.getAttributes().get("simpleAttribute");
        assertThat(simpleAttribute.getDataType()).isEqualTo("java.lang.String");
        assertThat(serializableEntity.getUniques()).containsExactly(Collections.singletonList("simpleAttribute"));
        assertThat(serializableEntity.getNextId()).isEqualTo(1L);
        assertThat(serializableEntity.getInstances()).hasSize(1);
        SerializableInstance serializableInstance = serializableEntity.getInstances().stream().findFirst().get();
        assertThat(serializableInstance.getId()).isEqualTo(0L);
        assertThat(serializableInstance.getValues()).containsOnlyKeys("simpleAttribute");
        assertThat(serializableInstance.getValues().get("simpleAttribute")).isEqualTo("simpleAttributeValue");
    }
}

