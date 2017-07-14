package io.axway.iron.core.internal.definition.entity;

import java.lang.reflect.Method;

public class RelationDefinition {
    private final Method m_relationMethod;
    private final String m_relationName;
    private final Class<?> m_tailEntityClass;
    private final Class<?> m_headEntityClass;
    private final RelationCardinality m_relationCardinality;
    private ReverseRelationDefinition m_reverseRelationDefinition = null;

    RelationDefinition(Method relationMethod, String relationName, Class<?> tailEntityClass, Class<?> headEntityClass,
                       RelationCardinality relationCardinality) {
        m_relationMethod = relationMethod;
        m_relationName = relationName;
        m_tailEntityClass = tailEntityClass;
        m_headEntityClass = headEntityClass;
        m_relationCardinality = relationCardinality;
    }

    // needed to break circular relation graph
    void setReverseRelationDefinition(ReverseRelationDefinition reverseRelationDefinition) {
        m_reverseRelationDefinition = reverseRelationDefinition;
    }

    public Method getRelationMethod() {
        return m_relationMethod;
    }

    public String getRelationName() {
        return m_relationName;
    }

    public Class<?> getTailEntityClass() {
        return m_tailEntityClass;
    }

    public Class<?> getHeadEntityClass() {
        return m_headEntityClass;
    }

    public RelationCardinality getRelationCardinality() {
        return m_relationCardinality;
    }

    public ReverseRelationDefinition getReverseRelationDefinition() {
        return m_reverseRelationDefinition;
    }
}
