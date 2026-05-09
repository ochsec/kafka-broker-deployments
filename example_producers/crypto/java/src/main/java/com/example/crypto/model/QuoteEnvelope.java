package com.example.crypto.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Envelope containing a timestamped quote from an exchange.
 * The raw broker message is stored as-is in the 'raw' field.
 */
public class QuoteEnvelope {
    private String exchange;
    private String symbol;

    @JsonProperty("received_at_ms")
    private long receivedAtMs;

    private JsonNode raw;

    public QuoteEnvelope() {
    }

    public QuoteEnvelope(String exchange, String symbol, long receivedAtMs, JsonNode raw) {
        this.exchange = exchange;
        this.symbol = symbol;
        this.receivedAtMs = receivedAtMs;
        this.raw = raw;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public long getReceivedAtMs() {
        return receivedAtMs;
    }

    public void setReceivedAtMs(long receivedAtMs) {
        this.receivedAtMs = receivedAtMs;
    }

    public JsonNode getRaw() {
        return raw;
    }

    public void setRaw(JsonNode raw) {
        this.raw = raw;
    }

    @Override
    public String toString() {
        return "QuoteEnvelope{" +
                "exchange='" + exchange + '\'' +
                ", symbol='" + symbol + '\'' +
                ", receivedAtMs=" + receivedAtMs +
                ", raw=" + raw +
                '}';
    }
}
