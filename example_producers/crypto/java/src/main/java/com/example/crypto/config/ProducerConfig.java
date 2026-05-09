package com.example.crypto.config;

/**
 * Loads and validates configuration from environment variables.
 */
public class ProducerConfig {
    private final String bootstrapServers;
    private final String topic;
    private final String exchange;
    private final String symbol;

    private ProducerConfig(String bootstrapServers, String topic, String exchange, String symbol) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.exchange = exchange;
        this.symbol = symbol;
    }

    public static ProducerConfig fromEnv() {
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS");
        String topic = getEnv("KAFKA_TOPIC");
        String exchange = getEnv("EXCHANGE");
        String symbol = getEnv("SYMBOL");

        validateExchange(exchange);
        validateSymbol(exchange, symbol);

        return new ProducerConfig(bootstrapServers, topic, exchange, symbol);
    }

    private static String getEnv(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Environment variable '" + key + "' is required but not set");
        }
        return value;
    }

    private static void validateExchange(String exchange) {
        if (!exchange.equals("coinbase") && !exchange.equals("kraken") && !exchange.equals("okx")) {
            throw new IllegalArgumentException(
                "EXCHANGE must be 'coinbase', 'kraken', or 'okx', got: " + exchange);
        }
    }

    private static void validateSymbol(String exchange, String symbol) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("SYMBOL is required but not set");
        }
        // Basic validation: symbol should contain a pair separator
        if (!symbol.contains("-") && !symbol.contains("/")) {
            throw new IllegalArgumentException(
                "SYMBOL must contain '-' or '/' (e.g., 'BTC-USD' or 'BTC/USD'), got: " + symbol);
        }
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public String getExchange() {
        return exchange;
    }

    public String getSymbol() {
        return symbol;
    }

    @Override
    public String toString() {
        return "ProducerConfig{" +
                "bootstrapServers='" + bootstrapServers + '\'' +
                ", topic='" + topic + '\'' +
                ", exchange='" + exchange + '\'' +
                ", symbol='" + symbol + '\'' +
                '}';
    }
}
