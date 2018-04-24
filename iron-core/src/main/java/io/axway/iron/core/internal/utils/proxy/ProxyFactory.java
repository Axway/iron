package io.axway.iron.core.internal.utils.proxy;

public interface ProxyFactory<T, C> {
    T createProxy(C proxyContext);
}
