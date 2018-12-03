package io.axway.iron.core.internal.utils.proxy;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import io.axway.iron.error.StoreException;

import static io.axway.alf.assertion.Assertion.checkArgument;
import static java.lang.invoke.MethodType.methodType;

class DefaultMethodCallHandler {

    static <T> MethodCallHandler<T> createDefaultMethodCallHandler(Method defaultMethod) {
        checkArgument(defaultMethod.isDefault(), "Method is not a default method", args -> args.add("method", defaultMethod));

        Class<?> declaringClass = defaultMethod.getDeclaringClass();
        MethodHandle methodHandle;
        try {
            MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(declaringClass, MethodHandles.lookup());
            methodHandle = lookup.findSpecial(declaringClass, defaultMethod.getName(), methodType(defaultMethod.getReturnType(), defaultMethod.getParameterTypes()), declaringClass);
        } catch (ReflectiveOperationException e) {
            throw new StoreException(e);
        }
        return (ctx, proxy, method, args) -> methodHandle.bindTo(proxy).invokeWithArguments(args);
    }

    private DefaultMethodCallHandler() {
    }
}
