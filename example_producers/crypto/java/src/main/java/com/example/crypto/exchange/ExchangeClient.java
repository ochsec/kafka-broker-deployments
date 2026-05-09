package com.example.crypto.exchange;

import com.example.crypto.producer.KafkaQuoteProducer;

/**
 * Interface for exchange-specific WebSocket clients.
 */
public interface ExchangeClient {
    /**
     * Connect to the exchange's WebSocket and start streaming quotes.
     * This method blocks until an error occurs or the client is stopped.
     *
     * @param producer the Kafka producer to send quotes to
     */
    void connect(KafkaQuoteProducer producer) throws Exception;

    /**
     * Gracefully shutdown the client and close any connections.
     */
    void shutdown();

    /**
     * Returns true if the client is still running.
     */
    boolean isRunning();
}
