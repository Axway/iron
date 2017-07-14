package io.axway.iron.core.internal.command;

import java.util.*;

class CommandProxyContext {
    private final Map<String, Object> m_parameters;

    CommandProxyContext(Map<String, Object> parameters) {
        m_parameters = parameters;
    }

    Map<String, Object> getParameters() {
        return m_parameters;
    }
}
