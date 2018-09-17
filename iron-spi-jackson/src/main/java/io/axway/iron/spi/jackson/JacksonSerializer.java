package io.axway.iron.spi.jackson;

import java.io.*;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import io.axway.iron.spi.model.snapshot.SerializableSnapshot;
import io.axway.iron.spi.model.transaction.SerializableTransaction;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;

public class JacksonSerializer implements TransactionSerializer, SnapshotSerializer {
    private final ObjectMapper m_objectMapper;

    JacksonSerializer() {
        m_objectMapper = new ObjectMapper();
        m_objectMapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(TypeFactory.defaultInstance()));
        m_objectMapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
        m_objectMapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    @Override
    public void serializeTransaction(OutputStream out, SerializableTransaction transaction) throws IOException {
        m_objectMapper.writer().writeValues(out).write(transaction);
    }

    @Override
    public SerializableTransaction deserializeTransaction(InputStream in) throws IOException {
        return m_objectMapper.reader().forType(SerializableTransaction.class).readValue(in);
    }

    @Override
    public void serializeSnapshot(OutputStream out, SerializableSnapshot serializableSnapshot) throws IOException {
        m_objectMapper.writer().writeValues(out).write(serializableSnapshot);
    }

    @Override
    public SerializableSnapshot deserializeSnapshot(String storeName, InputStream in) throws IOException {
        return m_objectMapper.reader().forType(SerializableSnapshot.class).readValue(in);
    }
}
