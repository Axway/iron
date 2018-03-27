package io.axway.iron.core.internal.utils.proxy;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import io.axway.iron.error.StoreException;

import static io.axway.alf.assertion.Assertion.checkArgument;

class DefaultMethodCallHandler {
    private static final Constructor<MethodHandles.Lookup> METHOD_HANDLES_LOOKUP_CONSTRUCTOR;

    static {
        Constructor<MethodHandles.Lookup> constructor = null;
        try {
            constructor = MethodHandles.Lookup.class.getDeclaredConstructor(Class.class, int.class);
            if (!constructor.isAccessible()) {
                constructor.setAccessible(true);
            }
        } catch (NoSuchMethodException e) {
            // ignore
        } finally {
            METHOD_HANDLES_LOOKUP_CONSTRUCTOR = constructor;
        }
    }

    static <T> MethodCallHandler<T> createDefaultMethodCallHandler(Method defaultMethod) {
        checkArgument(defaultMethod.isDefault(), "Method is not a default method", args -> args.add("method", defaultMethod));

        Class<?> declaringClass = defaultMethod.getDeclaringClass();
        MethodHandle methodHandle;
        try {
            methodHandle = METHOD_HANDLES_LOOKUP_CONSTRUCTOR.newInstance(declaringClass, MethodHandles.Lookup.PRIVATE)
                    .unreflectSpecial(defaultMethod, declaringClass);
        } catch (ReflectiveOperationException e) {
            throw new StoreException(e);
        }
        return (ctx, proxy, method, args) -> methodHandle.bindTo(proxy).invokeWithArguments(args);
    }

    private DefaultMethodCallHandler() {
    }
}
