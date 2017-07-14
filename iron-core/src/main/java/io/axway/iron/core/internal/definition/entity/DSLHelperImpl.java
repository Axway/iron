package io.axway.iron.core.internal.definition.entity;

import java.util.*;
import com.google.common.collect.ImmutableList;
import io.axway.iron.core.internal.utils.IntrospectionHelper;
import io.axway.iron.description.hook.DSLHelper;
import io.axway.iron.functional.Accessor;

class DSLHelperImpl implements DSLHelper {
    private final IntrospectionHelper m_introspectionHelper;

    private Class<?> m_tailEntityClass;
    private String m_methodName;

    DSLHelperImpl(IntrospectionHelper introspectionHelper) {
        m_introspectionHelper = introspectionHelper;
    }

    @Override
    public <TAIL, HEAD> Collection<TAIL> reciprocalManyRelation(Class<TAIL> tailEntityClass, Accessor<TAIL, HEAD> relationAccessor) {
        m_tailEntityClass = tailEntityClass;
        m_methodName = m_introspectionHelper.getMethodName(tailEntityClass, relationAccessor);

        return ImmutableList.of();
    }

    Class<?> getTailEntityClass() {
        return m_tailEntityClass;
    }

    String getMethodName() {
        return m_methodName;
    }
}
