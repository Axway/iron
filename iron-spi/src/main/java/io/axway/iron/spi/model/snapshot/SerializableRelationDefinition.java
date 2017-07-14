package io.axway.iron.spi.model.snapshot;

import javax.xml.bind.annotation.*;

@XmlType(propOrder = {"headEntityName", "cardinality"})
public class SerializableRelationDefinition {
    private String m_headEntityName;
    private SerializableRelationCardinality m_cardinality;

    public String getHeadEntityName() {
        return m_headEntityName;
    }

    public void setHeadEntityName(String headEntityName) {
        m_headEntityName = headEntityName;
    }

    public SerializableRelationCardinality getCardinality() {
        return m_cardinality;
    }

    public void setCardinality(SerializableRelationCardinality cardinality) {
        m_cardinality = cardinality;
    }
}
