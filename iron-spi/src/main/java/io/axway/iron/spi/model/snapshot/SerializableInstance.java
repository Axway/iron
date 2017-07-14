package io.axway.iron.spi.model.snapshot;

import java.util.*;
import javax.xml.bind.annotation.*;

@XmlType(propOrder = {"id", "values"})
public class SerializableInstance {
    private long m_id;
    private Map<String, Object> m_values;

    public long getId() {
        return m_id;
    }

    public void setId(long id) {
        m_id = id;
    }

    public Map<String, Object> getValues() {
        return m_values;
    }

    public void setValues(Map<String, Object> values) {
        m_values = values;
    }
}
