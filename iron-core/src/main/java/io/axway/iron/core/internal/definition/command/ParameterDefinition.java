package io.axway.iron.core.internal.definition.command;

import java.lang.reflect.Method;
import io.axway.iron.core.internal.utils.TypeConverter;

public class ParameterDefinition<T> {
    private final Method m_parameterMethod;
    private final String m_parameterName;
    private final Class<?> m_dataType;
    private final boolean m_multiple;
    private final boolean m_nullable;
    private final TypeConverter<T> m_typeConverter;

    ParameterDefinition(Method parameterMethod, String parameterName, Class<?> dataType, boolean multiple, boolean nullable, TypeConverter<T> typeConverter) {
        m_parameterMethod = parameterMethod;
        m_parameterName = parameterName;
        m_dataType = dataType;
        m_multiple = multiple;
        m_nullable = nullable;
        m_typeConverter = typeConverter;
    }

    public Method getParameterMethod() {
        return m_parameterMethod;
    }

    public String getParameterName() {
        return m_parameterName;
    }

    public Class<?> getDataType() {
        return m_dataType;
    }

    public boolean isMultiple() {
        return m_multiple;
    }

    public boolean isNullable() {
        return m_nullable;
    }

    public TypeConverter<T> getTypeConverter() {
        return m_typeConverter;
    }
}
