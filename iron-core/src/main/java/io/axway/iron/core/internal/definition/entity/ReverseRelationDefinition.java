package io.axway.iron.core.internal.definition.entity;

import java.lang.reflect.Method;

public class ReverseRelationDefinition {
    private final Method m_reverseRelationMethod;
    private RelationDefinition m_relationDefinition;

    ReverseRelationDefinition(Method reverseRelationMethod) {
        m_reverseRelationMethod = reverseRelationMethod;
    }

    // needed to break circular relation graph
    void setRelationDefinition(RelationDefinition relationDefinition) {
        m_relationDefinition = relationDefinition;
    }

    public Method getReverseRelationMethod() {
        return m_reverseRelationMethod;
    }

    public RelationDefinition getRelationDefinition() {
        return m_relationDefinition;
    }
}
