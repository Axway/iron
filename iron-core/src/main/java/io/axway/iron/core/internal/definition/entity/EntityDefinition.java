package io.axway.iron.core.internal.definition.entity;

import java.lang.reflect.Constructor;
import java.util.*;
import javax.annotation.*;

public class EntityDefinition<E> {
    private final Class<E> m_entityClass;
    private final IdDefinition m_idDefinition;
    private final Map<String, RelationDefinition> m_relations;
    private final Map<String, ReverseRelationDefinition> m_reverseRelations;
    private final Map<String, AttributeDefinition<Object>> m_attributes;
    private final List<String> m_uniqueConstraints;

    private final Constructor<E> m_instanceProxyConstructor;

    EntityDefinition(Class<E> entityClass, @Nullable IdDefinition idDefinition, Map<String, RelationDefinition> relations,
                     Map<String, ReverseRelationDefinition> reverseRelations, Map<String, AttributeDefinition<Object>> attributes,
                     List<String> uniqueConstraints, Constructor<E> instanceProxyConstructor) {
        m_entityClass = entityClass;
        m_idDefinition = idDefinition;
        m_relations = relations;
        m_reverseRelations = reverseRelations;
        m_attributes = attributes;
        m_uniqueConstraints = uniqueConstraints;
        m_instanceProxyConstructor = instanceProxyConstructor;
    }

    public String getEntityName() {
        return m_entityClass.getName();
    }

    public Class<E> getEntityClass() {
        return m_entityClass;
    }

    @Nullable
    public IdDefinition getIdDefinition() {
        return m_idDefinition;
    }

    public Map<String, RelationDefinition> getRelations() {
        return m_relations;
    }

    public Map<String, ReverseRelationDefinition> getReverseRelations() {
        return m_reverseRelations;
    }

    public Map<String, AttributeDefinition<Object>> getAttributes() {
        return m_attributes;
    }

    public List<String> getUniqueConstraints() {
        return m_uniqueConstraints;
    }

    public Constructor<E> getInstanceProxyConstructor() {
        return m_instanceProxyConstructor;
    }
}
