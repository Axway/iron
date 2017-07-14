package io.axway.iron.core.store.datatype.model;

import io.axway.iron.description.Entity;

@Entity
public interface PrimitiveTypeEntity {
    boolean booleanValue();

    byte byteValue();

    char charValue();

    short shortValue();

    int intValue();

    long longValue();

    float floatValue();

    double doubleValue();
}
