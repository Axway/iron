package io.axway.iron.core.internal.definition.command;

import java.lang.reflect.Constructor;
import java.util.*;
import io.axway.iron.Command;

public class CommandDefinition<C extends Command<?>> {
    private final Class<C> m_commandClass;
    private final Map<String, ParameterDefinition<Object>> m_parameters;

    private final Constructor<C> m_commandProxyConstructor;

    CommandDefinition(Class<C> commandClass, Map<String, ParameterDefinition<Object>> parameters, Constructor<C> commandProxyConstructor) {
        m_commandClass = commandClass;
        m_parameters = parameters;
        m_commandProxyConstructor = commandProxyConstructor;
    }

    public Class<C> getCommandClass() {
        return m_commandClass;
    }

    public Map<String, ParameterDefinition<Object>> getParameters() {
        return m_parameters;
    }

    public Constructor<C> getCommandProxyConstructor() {
        return m_commandProxyConstructor;
    }
}
