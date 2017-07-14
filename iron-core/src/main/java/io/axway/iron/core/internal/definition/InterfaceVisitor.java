package io.axway.iron.core.internal.definition;

import java.lang.reflect.Method;

public interface InterfaceVisitor {
    void visitInterface(Class<?> clazz);

    boolean shouldVisitMethod(Method method);

    void visitMethod(Method method, Class<?> dataType, boolean multiple, boolean nullable);
}
