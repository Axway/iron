package io.axway.iron.core.internal.utils.proxy;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.*;
import com.google.common.collect.ImmutableMap;
import io.axway.iron.error.StoreException;

import static io.axway.iron.core.internal.utils.proxy.DefaultMethodCallHandler.createDefaultMethodCallHandler;

public class ProxyFactoryBuilder<C> {
    private static final Method OBJECT_EQUALS_METHOD;
    private static final Method OBJECT_HASHCODE_METHOD;
    private static final Method OBJECT_TO_STRING_METHOD;

    static {
        try {
            OBJECT_EQUALS_METHOD = Object.class.getMethod("equals", Object.class);
            OBJECT_HASHCODE_METHOD = Object.class.getMethod("hashCode");
            OBJECT_TO_STRING_METHOD = Object.class.getMethod("toString");
        } catch (NoSuchMethodException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final ImmutableMap.Builder<Method, MethodCallHandler<C>> m_builder = ImmutableMap.builder();
    private MethodCallHandler<C> m_unhandledMethodCallHandler = (ctx, proxy, method, args) -> {
        throw new StoreException("Method is not implemented by this proxy", a -> a.add("methodName", method));
    };

    public static <C> ProxyFactoryBuilder<C> newProxyFactoryBuilder() {
        return new ProxyFactoryBuilder<>();
    }

    public ProxyFactoryBuilder<C> handleDefaultMethod(Method method) {
        m_builder.put(method, createDefaultMethodCallHandler(method));
        return this;
    }

    public ProxyFactoryBuilder<C> handleObjectEquals(MethodCallHandler<C> objectEqualsCallHandler) {
        m_builder.put(OBJECT_EQUALS_METHOD, objectEqualsCallHandler);
        return this;
    }

    public ProxyFactoryBuilder<C> handleObjectHashcode(MethodCallHandler<C> objectEqualsCallHandler) {
        m_builder.put(OBJECT_HASHCODE_METHOD, objectEqualsCallHandler);
        return this;
    }

    public ProxyFactoryBuilder<C> handleObjectToString(MethodCallHandler<C> objectEqualsCallHandler) {
        m_builder.put(OBJECT_TO_STRING_METHOD, objectEqualsCallHandler);
        return this;
    }

    public ProxyFactoryBuilder<C> defaultObjectEquals() {
        return handleObjectEquals((ctx, proxy, method, args) -> proxy == args[0]);
    }

    public ProxyFactoryBuilder<C> defaultObjectHashcode() {
        return handleObjectHashcode((ctx, proxy, method, args) -> System.identityHashCode(proxy));
    }

    public ProxyFactoryBuilder<C> defaultObjectToString() {
        return handleObjectToString((ctx, proxy, method, args) -> proxy.getClass().getName() + "@" + Integer.toHexString(proxy.hashCode()));
    }

    public ProxyFactoryBuilder<C> defaultObjectMethods() {
        return defaultObjectEquals().defaultObjectHashcode().defaultObjectToString();
    }

    public ProxyFactoryBuilder<C> handle(Method method, MethodCallHandler<C> methodCallHandler) {
        m_builder.put(method, methodCallHandler);
        return this;
    }

    public ProxyFactoryBuilder<C> unhandled(MethodCallHandler<C> unhandledMethodCallHandler) {
        m_unhandledMethodCallHandler = unhandledMethodCallHandler;
        return this;
    }

    public <T> ProxyFactory<T, C> build(Class<T> clazz) {
        Constructor<T> constructor = ProxyConstructorFactory.createProxyConstructor(clazz);
        return build(constructor);
    }

    public <T> ProxyFactory<T, C> build(Constructor<T> proxyConstructor) {
        Map<Method, MethodCallHandler<C>> handlers = m_builder.build();
        MethodCallHandler<C> methodCallHandler = (ctx, proxy, method, args) -> {
            MethodCallHandler<C> callHandler = handlers.get(method);
            if (callHandler == null) {
                callHandler = m_unhandledMethodCallHandler;
            }
            return callHandler.invoke(ctx, proxy, method, args);
        };
        return new ProxyFactoryImpl<>(methodCallHandler, proxyConstructor);
    }

    private static class ProxyFactoryImpl<T, C> implements ProxyFactory<T, C> {
        private final MethodCallHandler<C> m_methodCallHandler;
        private final Constructor<T> m_proxyConstructor;

        ProxyFactoryImpl(MethodCallHandler<C> methodCallHandler, Constructor<T> proxyConstructor) {
            m_methodCallHandler = methodCallHandler;
            m_proxyConstructor = proxyConstructor;
        }

        @Override
        public T createProxy(C proxyContext) {
            InvocationHandler h = (proxy, method, args) -> m_methodCallHandler.invoke(proxyContext, proxy, method, args);
            try {
                return m_proxyConstructor.newInstance(h);
            } catch (ReflectiveOperationException e) {
                throw new StoreException(e);
            }
        }
    }
}
