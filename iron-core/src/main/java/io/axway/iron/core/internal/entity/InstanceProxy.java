package io.axway.iron.core.internal.entity;

import javax.annotation.*;

@SuppressWarnings("WeakerAccess") // this is necessary to avoid classloading/visibility problems for proxy generation in OSGi
public interface InstanceProxy {
    long __id();

    @Nullable
    Object __get(String key);

    Object __set(String key, @Nullable Object value);

    <E> Class<E> __entityClass();
}
