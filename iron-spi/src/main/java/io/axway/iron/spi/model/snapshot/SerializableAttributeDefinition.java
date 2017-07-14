package io.axway.iron.spi.model.snapshot;

import javax.xml.bind.annotation.*;

@XmlType(propOrder = {"dataType", "nullable"})
public class SerializableAttributeDefinition {
    private String m_dataType;
    private boolean m_nullable;

    public String getDataType() {
        return m_dataType;
    }

    public void setDataType(String dataType) {
        m_dataType = dataType;
    }

    public boolean isNullable() {
        return m_nullable;
    }

    public void setNullable(boolean nullable) {
        m_nullable = nullable;
    }
}
