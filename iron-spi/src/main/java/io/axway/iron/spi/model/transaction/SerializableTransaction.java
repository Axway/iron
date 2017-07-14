package io.axway.iron.spi.model.transaction;

import java.util.*;
import javax.xml.bind.annotation.*;

@XmlType(propOrder = {"transactionModelVersion", "synchronizationId", "commands"})
public class SerializableTransaction {
    public static final long TRANSACTION_MODEL_VERSION = 1;

    private long m_transactionModelVersion;
    private String m_synchronizationId;
    private List<SerializableCommand> m_commands;

    public long getTransactionModelVersion() {
        return m_transactionModelVersion;
    }

    public void setTransactionModelVersion(long transactionModelVersion) {
        m_transactionModelVersion = transactionModelVersion;
    }

    public String getSynchronizationId() {
        return m_synchronizationId;
    }

    public void setSynchronizationId(String synchronizationId) {
        m_synchronizationId = synchronizationId;
    }

    public List<SerializableCommand> getCommands() {
        return m_commands;
    }

    public void setCommands(List<SerializableCommand> commands) {
        m_commands = commands;
    }
}
