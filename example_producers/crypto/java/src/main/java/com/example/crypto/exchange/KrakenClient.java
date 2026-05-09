package com.example.crypto.exchange;

import com.example.crypto.producer.KafkaQuoteProducer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kraken WebSocket client for streaming crypto quotes.
 * Kraken provides automatic heartbeats, so we use a timeout to detect disconnections.
 */
public class KrakenClient implements ExchangeClient {
    private static final Logger logger = LoggerFactory.getLogger(KrakenClient.class);
    private static final String WS_URL = "wss://ws.kraken.com/v2";
    private static final long HEARTBEAT_TIMEOUT_MS = 35000; // 35 seconds

    private final String symbol;
    private final ObjectMapper mapper;
    private volatile boolean running;
    private KrakenWebSocket webSocket;

    public KrakenClient(String symbol) {
        this.symbol = symbol;
        this.mapper = new ObjectMapper();
        this.running = true;
    }

    @Override
    public void connect(KafkaQuoteProducer producer) throws Exception {
        try {
            webSocket = new KrakenWebSocket(new URI(WS_URL), symbol, producer, mapper);
            webSocket.connectBlocking(10, TimeUnit.SECONDS);
            logger.info("Connected to Kraken WebSocket");

            // Keep the connection alive and monitor for timeouts
            AtomicLong lastHeartbeat = new AtomicLong(System.currentTimeMillis());
            webSocket.setHeartbeatCallback(() -> lastHeartbeat.set(System.currentTimeMillis()));

            while (running && webSocket.isOpen()) {
                long timeSinceLastHeartbeat = System.currentTimeMillis() - lastHeartbeat.get();
                if (timeSinceLastHeartbeat > HEARTBEAT_TIMEOUT_MS) {
                    logger.warn("Heartbeat timeout detected, reconnecting...");
                    running = false;
                    break;
                }
                Thread.sleep(1000);
            }

            logger.info("Kraken WebSocket disconnected");
        } catch (Exception e) {
            logger.error("Error connecting to Kraken", e);
            running = false;
            throw e;
        }
    }

    @Override
    public void shutdown() {
        logger.info("Shutting down Kraken client");
        running = false;
        if (webSocket != null && webSocket.isOpen()) {
            webSocket.close();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * Inner class: the actual WebSocket implementation for Kraken.
     */
    private static class KrakenWebSocket extends WebSocketClient {
        private final String symbol;
        private final KafkaQuoteProducer producer;
        private final ObjectMapper mapper;
        private Runnable heartbeatCallback;
        private volatile boolean subscribed = false;

        public KrakenWebSocket(URI serverUri, String symbol, KafkaQuoteProducer producer, ObjectMapper mapper) {
            super(serverUri);
            this.symbol = symbol;
            this.producer = producer;
            this.mapper = mapper;
        }

        public void setHeartbeatCallback(Runnable callback) {
            this.heartbeatCallback = callback;
        }

        @Override
        public void onOpen(ServerHandshake handshakedata) {
            logger.info("Kraken WebSocket opened");
            subscribeToTicker();
        }

        @Override
        public void onMessage(String message) {
            try {
                JsonNode msg = mapper.readTree(message);
                String channel = msg.path("channel").asText();

                if ("heartbeat".equals(channel)) {
                    if (heartbeatCallback != null) {
                        heartbeatCallback.run();
                    }
                    logger.debug("Received heartbeat from Kraken");
                } else if ("ticker".equals(channel)) {
                    handleTickerMessage(msg);
                }
            } catch (Exception e) {
                logger.error("Error processing Kraken message", e);
            }
        }

        @Override
        public void onClose(int code, String reason, boolean remote) {
            logger.warn("Kraken WebSocket closed. Code: {}, Reason: {}", code, reason);
        }

        @Override
        public void onError(Exception ex) {
            logger.error("Kraken WebSocket error", ex);
        }

        private void subscribeToTicker() {
            try {
                String subscription = "{\"method\":\"subscribe\",\"params\":{\"channel\":\"ticker\",\"symbol\":[\"" + symbol + "\"]}}";
                send(subscription);
                logger.info("Subscribed to ticker for {}", symbol);
                subscribed = true;
            } catch (Exception e) {
                logger.error("Error subscribing to Kraken ticker", e);
            }
        }

        private void handleTickerMessage(JsonNode msg) {
            try {
                JsonNode data = msg.path("data");
                if (data.isArray() && data.size() > 0) {
                    // Kraken sends ticker data as an array; we take the first element
                    JsonNode ticker = data.get(0);
                    String tickerSymbol = ticker.path("symbol").asText();
                    // Send the entire ticker node to Kafka
                    producer.sendQuote(tickerSymbol, ticker);
                }
            } catch (Exception e) {
                logger.error("Error handling Kraken ticker message", e);
            }
        }
    }
}
