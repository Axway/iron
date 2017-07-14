package io.axway.iron.core.internal.utils.proxy;

public interface ProxyFactory<T, C> {
    NoContext NO_CONTEXT = new NoContext() {
    };

    interface NoContext {
    }

    T createProxy(C proxyContext);
}
