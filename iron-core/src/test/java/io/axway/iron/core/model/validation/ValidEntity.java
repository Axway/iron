package io.axway.iron.core.model.validation;

import java.util.*;
import javax.annotation.*;
import io.axway.iron.description.Entity;
import io.axway.iron.description.Id;
import io.axway.iron.description.Transient;
import io.axway.iron.description.Unique;

@Entity
public interface ValidEntity {

    @Id
    long internalId();

    @Unique
    String id();

    TargetWithReverseEntity relation1();

    @Nullable
    TargetWithReverseEntity relation2();

    Collection<TargetWithReverseEntity> relations();

    String stringNonnull();

    @Nullable
    String stringNullable();

    Date dateNonnull();

    @Nullable
    Date dateNullable();

    @Nullable
    Boolean booleanWrapper();

    boolean booleanNonWrapper();

    @Nullable
    Byte byteWrapper();

    byte byteNonWrapper();

    @Nullable
    Character charWrapper();

    char charNonWrapper();

    @Nullable
    Short shortWrapper();

    short shortNonWrapper();

    @Nullable
    Integer intWrapper();

    int intNonWrapper();

    @Nullable
    Long longWrapper();

    long longNonWrapper();

    @Nullable
    Float floatWrapper();

    float floatNonWrapper();

    @Nullable
    Double doubleWrapper();

    double doubleNonWrapper();

    @Transient
    default String test() {
        return id() + stringNonnull();
    }

    static boolean haveSameValue(ValidEntity e1, ValidEntity e2) {
        return Objects.equals(e1.stringNonnull(), e2.stringNonnull());
    }
}
