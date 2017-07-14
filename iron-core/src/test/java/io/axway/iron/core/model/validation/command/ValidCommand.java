package io.axway.iron.core.model.validation.command;

import java.util.*;
import javax.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.description.Transient;

public interface ValidCommand extends Command<Void> {

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

    Collection<String> values();

    @Override
    default Void execute(@Nonnull ReadWriteTransaction tx) {
        return null;
    }

    @Transient
    default String test() {
        return stringNonnull() + stringNullable();
    }

    static String concat(String s1, String s2) {
        return s1 + s2;
    }
}
