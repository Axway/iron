package io.axway.iron.spi.consul;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

class ConsulOperation {
    @JsonProperty("Verb")
    private String m_verb;

    @JsonProperty("Key")
    private String m_key;

    @JsonProperty("Value")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private byte[] m_value;

    @JsonProperty("Flags")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String m_flags;

    @JsonProperty("Index")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String m_index;

    @JsonProperty("Session")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String m_session;

    String getVerb() {
        return m_verb;
    }

    void setVerb(String verb) {
        m_verb = verb;
    }

    String getKey() {
        return m_key;
    }

    void setKey(String key) {
        m_key = key;
    }

    byte[] getValue() {
        return m_value;
    }

    void setValue(byte[] value) {
        m_value = value;
    }

    String getFlags() {
        return m_flags;
    }

    void setFlags(String flags) {
        m_flags = flags;
    }

    String getIndex() {
        return m_index;
    }

    void setIndex(String index) {
        m_index = index;
    }

    String getSession() {
        return m_session;
    }

    void setSession(String session) {
        m_session = session;
    }
}
