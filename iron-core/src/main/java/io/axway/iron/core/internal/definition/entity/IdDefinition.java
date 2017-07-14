package io.axway.iron.core.internal.definition.entity;

import java.lang.reflect.Method;

public class IdDefinition {
    private final Method m_idMethod;
    private final String m_idName;

    IdDefinition(Method idMethod, String idName) {
        m_idMethod = idMethod;
        m_idName = idName;
    }

    public Method getIdMethod() {
        return m_idMethod;
    }

    public String getIdName() {
        return m_idName;
    }
}
