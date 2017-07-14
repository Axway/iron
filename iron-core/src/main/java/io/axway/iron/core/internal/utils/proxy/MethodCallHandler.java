package io.axway.iron.core.internal.utils.proxy;

import java.lang.reflect.Method;

public interface MethodCallHandler<C> {
    Object invoke(C context, Object proxy, Method method, Object[] args) throws Throwable;
}
