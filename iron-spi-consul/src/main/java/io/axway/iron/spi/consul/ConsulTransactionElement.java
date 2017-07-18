package io.axway.iron.spi.consul;

import com.fasterxml.jackson.annotation.JsonProperty;

class ConsulTransactionElement {
    @JsonProperty("KV")
    private ConsulOperation m_kv;

    ConsulOperation getKV() {
        return m_kv;
    }

    void setKV(ConsulOperation kv) {
        m_kv = kv;
    }
}
