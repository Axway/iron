package io.axway.iron.spi.model.transaction;

import java.util.*;
import javax.xml.bind.annotation.*;

@XmlType(propOrder = {"commandName", "parameters"})
public class SerializableCommand {
    private String m_commandName;
    private Map<String, Object> m_parameters;

    public String getCommandName() {
        return m_commandName;
    }

    public void setCommandName(String commandName) {
        m_commandName = commandName;
    }

    public Map<String, Object> getParameters() {
        return m_parameters;
    }

    public void setParameters(Map<String, Object> parameters) {
        m_parameters = parameters;
    }
}
