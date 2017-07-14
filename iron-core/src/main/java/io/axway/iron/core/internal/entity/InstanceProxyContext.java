package io.axway.iron.core.internal.entity;

import java.util.*;
import javax.annotation.*;

class InstanceProxyContext {
    private final long m_id;
    private final Map<String, Object> m_attributes = new HashMap<>();

    InstanceProxyContext(long id) {
        m_id = id;
    }

    long getId() {
        return m_id;
    }

    Object getAttribute(String name) {
        return m_attributes.get(name);
    }

    Object setAttribute(String name, @Nullable Object value) {
        if (value != null) {
            return m_attributes.put(name, value);
        } else {
            return m_attributes.remove(name);
        }
    }

    Map<String, Object> getAttributes() {
        return m_attributes;
    }
}
