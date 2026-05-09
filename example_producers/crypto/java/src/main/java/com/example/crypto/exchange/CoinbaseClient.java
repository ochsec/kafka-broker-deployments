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

/**
 * Coinbase Advanced Trade WebSocket client for streaming BTC-USD quotes.
 * Subscribes to both ticker and heartbeats to maintain the connection.
 */
public class CoinbaseClient implements ExchangeClient {
    private static final Logger logger = LoggerFactory.getLogger(CoinbaseClient.class);
    private static final String WS_URL = "wss://advanced-trade-ws.coinbase.com";
    private static final String CHANNEL = "ticker";

    private final String symbol;
    private final ObjectMapper mapper;
    private volatile boolean running;
    private CoinbaseWebSocket webSocket;

    public CoinbaseClient(String symbol) {
        this.symbol = symbol;
        this.mapper = new ObjectMapper();
        this.running = true;
    }

    @Override
    public void connect(KafkaQuoteProducer producer) throws Exception {
        try {
            webSocket = new CoinbaseWebSocket(new URI(WS_URL), symbol, producer, mapper);
            webSocket.connectBlocking(10, TimeUnit.SECONDS);
            logger.info("Connected to Coinbase WebSocket");

            // Keep the connection alive
            while (running && webSocket.isOpen()) {
                Thread.sleep(1000);
            }

            logger.info("Coinbase WebSocket disconnected");
        } catch (Exception e) {
            logger.error("Error connecting to Coinbase", e);
            running = false;
            throw e;
        }
    }

    @Override
    public void shutdown() {
        logger.info("Shutting down Coinbase client");
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
     * Inner class: the actual WebSocket implementation for Coinbase.
     */
    private static class CoinbaseWebSocket extends WebSocketClient {
        private final String symbol;
        private final KafkaQuoteProducer producer;
        private final ObjectMapper mapper;
        private volatile boolean subscribed = false;

        public CoinbaseWebSocket(URI serverUri, String symbol, KafkaQuoteProducer producer, ObjectMapper mapper) {
            super(serverUri);
            this.symbol = symbol;
            this.producer = producer;
            this.mapper = mapper;
        }

        @Override
        public void onOpen(ServerHandshake handshakedata) {
            logger.info("Coinbase WebSocket opened");
            subscribeToTickerAndHeartbeats();
        }

        @Override
        public void onMessage(String message) {
            try {
                JsonNode msg = mapper.readTree(message);
                String channel = msg.path("channel").asText();

                if ("ticker".equals(channel)) {
                    handleTickerMessage(msg);
                } else if ("heartbeats".equals(channel)) {
                    logger.debug("Received heartbeat from Coinbase");
                }
            } catch (Exception e) {
                logger.error("Error processing Coinbase message", e);
            }
        }

        @Override
        public void onClose(int code, String reason, boolean remote) {
            logger.warn("Coinbase WebSocket closed. Code: {}, Reason: {}", code, reason);
        }

        @Override
        public void onError(Exception ex) {
            logger.error("Coinbase WebSocket error", ex);
        }

        private void subscribeToTickerAndHeartbeats() {
            try {
                // Subscribe to ticker
                String tickerSubscription = "{\"type\":\"subscribe\",\"product_ids\":[\"" + symbol + "\"],\"channel\":\"ticker\"}";
                send(tickerSubscription);
                logger.info("Subscribed to ticker for {}", symbol);

                // Subscribe to heartbeats to keep connection alive
                String heartbeatSubscription = "{\"type\":\"subscribe\",\"channel\":\"heartbeats\"}";
                send(heartbeatSubscription);
                logger.info("Subscribed to heartbeats");

                subscribed = true;
            } catch (Exception e) {
                logger.error("Error subscribing to Coinbase channels", e);
            }
        }

        private void handleTickerMessage(JsonNode msg) {
            try {
                JsonNode events = msg.path("events");
                if (events.isArray()) {
                    for (JsonNode event : events) {
                        String eventType = event.path("type").asText();
                        if ("snapshot".equals(eventType) || "update".equals(eventType)) {
                            JsonNode tickers = event.path("tickers");
                            if (tickers.isArray()) {
                                for (JsonNode ticker : tickers) {
                                    String productId = ticker.path("product_id").asText();
                                    // Send the entire ticker node to Kafka
                                    producer.sendQuote(productId, ticker);
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Error handling Coinbase ticker message", e);
            }
        }
    }
}
