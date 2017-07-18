package io.axway.iron.spi.consul;

import com.fasterxml.jackson.annotation.JsonProperty;

class ConsulKV {
    @JsonProperty("LockIndex")
    private String m_lockIndex;
    @JsonProperty("Key")
    private String m_key;
    @JsonProperty("Flags")
    private String m_flags;
    @JsonProperty("Value")
    private byte[] m_value;
    @JsonProperty("CreateIndex")
    private String m_createIndex;
    @JsonProperty("ModifyIndex")
    private String m_modifyIndex;

    String getLockIndex() {
        return m_lockIndex;
    }

    void setLockIndex(String lockIndex) {
        m_lockIndex = lockIndex;
    }

    String getKey() {
        return m_key;
    }

    void setKey(String key) {
        m_key = key;
    }

    String getFlags() {
        return m_flags;
    }

    void setFlags(String flags) {
        m_flags = flags;
    }

    byte[] getValue() {
        return m_value;
    }

    void setValue(byte[] value) {
        m_value = value;
    }

    String getCreateIndex() {
        return m_createIndex;
    }

    void setCreateIndex(String createIndex) {
        m_createIndex = createIndex;
    }

    String getModifyIndex() {
        return m_modifyIndex;
    }

    void setModifyIndex(String modifyIndex) {
        m_modifyIndex = modifyIndex;
    }
}
