package io.axway.iron.core.internal.definition.entity;

import java.lang.reflect.Method;
import io.axway.iron.core.internal.utils.TypeConverter;

public class AttributeDefinition<T> {
    private final Method m_attributeMethod;
    private final String m_attributeName;
    private final Class<?> m_dataType;
    private final boolean m_nullable;
    private final TypeConverter<T> m_typeConverter;

    AttributeDefinition(Method attributeMethod, String attributeName, Class<?> dataType, boolean nullable, TypeConverter<T> typeConverter) {
        m_attributeMethod = attributeMethod;
        m_attributeName = attributeName;
        m_dataType = dataType;
        m_nullable = nullable;
        m_typeConverter = typeConverter;
    }

    public Method getAttributeMethod() {
        return m_attributeMethod;
    }

    public String getAttributeName() {
        return m_attributeName;
    }

    public Class<?> getDataType() {
        return m_dataType;
    }

    public boolean isNullable() {
        return m_nullable;
    }

    public TypeConverter<T> getTypeConverter() {
        return m_typeConverter;
    }
}
