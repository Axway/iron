package io.axway.iron.core.internal.definition;

import java.lang.reflect.Method;

public interface InterfaceVisitor {
    void visitInterface(Class<?> clazz);

    boolean shouldVisitMethod(Method method);

    <T> void visitMethod(Method method, Class<T> dataType, boolean multiple, boolean nullable);
}
