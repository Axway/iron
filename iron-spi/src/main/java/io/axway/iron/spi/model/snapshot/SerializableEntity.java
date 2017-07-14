package io.axway.iron.spi.model.snapshot;

import java.util.*;
import javax.xml.bind.annotation.*;

@XmlType(propOrder = {"entityName", "relations", "attributes", "uniques", "nextId", "instances"})
public class SerializableEntity {
    private String m_entityName;
    private Map<String, SerializableRelationDefinition> m_relations;
    private Map<String, SerializableAttributeDefinition> m_attributes;
    private List<List<String>> m_uniques;

    private long m_nextId;
    private Collection<SerializableInstance> m_instances;

    public String getEntityName() {
        return m_entityName;
    }

    public void setEntityName(String entityName) {
        m_entityName = entityName;
    }

    public Map<String, SerializableRelationDefinition> getRelations() {
        return m_relations;
    }

    public void setRelations(Map<String, SerializableRelationDefinition> relations) {
        m_relations = relations;
    }

    public Map<String, SerializableAttributeDefinition> getAttributes() {
        return m_attributes;
    }

    public void setAttributes(Map<String, SerializableAttributeDefinition> attributes) {
        m_attributes = attributes;
    }

    public List<List<String>> getUniques() {
        return m_uniques;
    }

    public void setUniques(List<List<String>> uniques) {
        m_uniques = uniques;
    }

    public long getNextId() {
        return m_nextId;
    }

    public void setNextId(long nextId) {
        m_nextId = nextId;
    }

    public Collection<SerializableInstance> getInstances() {
        return m_instances;
    }

    public void setInstances(Collection<SerializableInstance> instances) {
        m_instances = instances;
    }
}
