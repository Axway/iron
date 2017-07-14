package io.axway.iron.core.internal.utils;

import javax.annotation.*;

public interface TypeConverter<T> {
    T convert(@Nullable Object value);
}
