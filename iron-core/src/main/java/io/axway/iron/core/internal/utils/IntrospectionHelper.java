package io.axway.iron.core.internal.utils;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.*;
import io.axway.iron.core.internal.utils.proxy.ProxyFactoryBuilder;
import io.axway.iron.error.StoreException;
import io.axway.iron.functional.Accessor;

import static io.axway.alf.assertion.Assertion.checkState;
import static java.util.Map.*;

public class IntrospectionHelper {
    private static final Map<Class<?>, Object> DEFAULT_VALUES = Map.ofEntries( //
                                                                               entry(boolean.class, Boolean.FALSE), //
                                                                               entry(byte.class, (byte) 0), //
                                                                               entry(char.class, '\0'), //
                                                                               entry(short.class, (short) 0), //
                                                                               entry(int.class, 0), //
                                                                               entry(long.class, 0L), //
                                                                               entry(float.class, 0f), //
                                                                               entry(double.class, 0d));

    private final ConcurrentMap<Accessor<?, ?>, String> m_methodReferenceNames = new ConcurrentHashMap<>();

    public Class<?> getParametrizedClass(Type genericReturnType, int index) {
        if (genericReturnType instanceof ParameterizedType) {
            Type[] actualTypeArguments = ((ParameterizedType) genericReturnType).getActualTypeArguments();
            if (actualTypeArguments.length > index) {
                Type typeArg = actualTypeArguments[index];
                if (typeArg instanceof Class) {
                    return (Class<?>) typeArg;
                }
            }
        }
        throw new StoreException("Unable to retrieve the parametrized class", args -> args.add("type", genericReturnType).add("index", index));
    }

    public <T> String getMethodName(Class<T> clazz, Accessor<T, ?> accessor) {
        return m_methodReferenceNames.computeIfAbsent(accessor, ignored -> retrieveMethod(clazz, accessor).getName());
    }

    /**
     * This method don't use cache. Use with care.<br>
     * If caching is required, then use {@link #getMethodName(Class, Accessor)}
     *
     * @param clazz the class where the method is defined
     * @param accessor the method reference
     * @param <T> the type of the class
     * @return the referenced method
     */
    public static <T> Method retrieveMethod(Class<T> clazz, Accessor<T, ?> accessor) {
        MethodCallRecorder methodCallRecorder = new MethodCallRecorder();

        T accessorInstance = ProxyFactoryBuilder.<MethodCallRecorder>newProxyFactoryBuilder() //
                .defaultObjectMethods() //
                .unhandled((context, proxy, method, args) -> {
                    context.setMethod(method);
                    return DEFAULT_VALUES.get(method.getReturnType());
                }) //
                .build(clazz).createProxy(methodCallRecorder);

        accessor.get(accessorInstance);
        Method method = methodCallRecorder.getMethod();
        if (method == null) {
            throw new StoreException("Method reference didn't call any method on the class", args -> args.add("className", clazz.getName()));
        }
        return method;
    }

    private static final class MethodCallRecorder {
        private Method m_method;

        void setMethod(Method method) {
            checkState(m_method == null, "Method has been already set");
            m_method = method;
        }

        Method getMethod() {
            return m_method;
        }
    }
}
