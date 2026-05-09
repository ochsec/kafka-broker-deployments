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
 * OKX WebSocket client for streaming crypto quotes.
 * OKX requires periodic ping/pong to maintain connection (every 25 seconds).
 */
public class OkxClient implements ExchangeClient {
    private static final Logger logger = LoggerFactory.getLogger(OkxClient.class);
    private static final String WS_URL = "wss://ws.okx.com:8443/ws/v5/public";
    private static final long PING_INTERVAL_MS = 25000; // 25 seconds

    private final String symbol;
    private final ObjectMapper mapper;
    private volatile boolean running;
    private OkxWebSocket webSocket;

    public OkxClient(String symbol) {
        this.symbol = symbol;
        this.mapper = new ObjectMapper();
        this.running = true;
    }

    @Override
    public void connect(KafkaQuoteProducer producer) throws Exception {
        try {
            webSocket = new OkxWebSocket(new URI(WS_URL), symbol, producer, mapper);
            webSocket.connectBlocking(10, TimeUnit.SECONDS);
            logger.info("Connected to OKX WebSocket");

            // Keep the connection alive by sending periodic pings
            AtomicLong lastPing = new AtomicLong(System.currentTimeMillis());

            while (running && webSocket.isOpen()) {
                long timeSinceLastPing = System.currentTimeMillis() - lastPing.get();
                if (timeSinceLastPing > PING_INTERVAL_MS) {
                    try {
                        webSocket.send("ping");
                        lastPing.set(System.currentTimeMillis());
                        logger.debug("Sent ping to OKX");
                    } catch (Exception e) {
                        logger.error("Error sending ping to OKX", e);
                        running = false;
                        break;
                    }
                }
                Thread.sleep(1000);
            }

            logger.info("OKX WebSocket disconnected");
        } catch (Exception e) {
            logger.error("Error connecting to OKX", e);
            running = false;
            throw e;
        }
    }

    @Override
    public void shutdown() {
        logger.info("Shutting down OKX client");
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
     * Inner class: the actual WebSocket implementation for OKX.
     */
    private static class OkxWebSocket extends WebSocketClient {
        private final String symbol;
        private final KafkaQuoteProducer producer;
        private final ObjectMapper mapper;
        private volatile boolean subscribed = false;

        public OkxWebSocket(URI serverUri, String symbol, KafkaQuoteProducer producer, ObjectMapper mapper) {
            super(serverUri);
            this.symbol = symbol;
            this.producer = producer;
            this.mapper = mapper;
        }

        @Override
        public void onOpen(ServerHandshake handshakedata) {
            logger.info("OKX WebSocket opened");
            subscribeToTicker();
        }

        @Override
        public void onMessage(String message) {
            try {
                // OKX sends "pong" as a text frame (not JSON)
                if ("pong".equals(message)) {
                    logger.debug("Received pong from OKX");
                    return;
                }

                JsonNode msg = mapper.readTree(message);
                String event = msg.path("event").asText();

                // Check for subscription confirmation
                if ("subscribe".equals(event)) {
                    logger.info("Subscription confirmed for {}", symbol);
                    return;
                }

                // Handle ticker data
                handleTickerMessage(msg);
            } catch (Exception e) {
                logger.error("Error processing OKX message", e);
            }
        }

        @Override
        public void onClose(int code, String reason, boolean remote) {
            logger.warn("OKX WebSocket closed. Code: {}, Reason: {}", code, reason);
        }

        @Override
        public void onError(Exception ex) {
            logger.error("OKX WebSocket error", ex);
        }

        private void subscribeToTicker() {
            try {
                String subscription = "{\"id\":\"1512\",\"op\":\"subscribe\",\"args\":[{\"channel\":\"tickers\",\"instId\":\"" + symbol + "\"}]}";
                send(subscription);
                logger.info("Subscribed to tickers for {}", symbol);
                subscribed = true;
            } catch (Exception e) {
                logger.error("Error subscribing to OKX tickers", e);
            }
        }

        private void handleTickerMessage(JsonNode msg) {
            try {
                String channel = msg.path("arg").path("channel").asText();
                if ("tickers".equals(channel)) {
                    JsonNode data = msg.path("data");
                    if (data.isArray()) {
                        for (JsonNode ticker : data) {
                            String instId = ticker.path("instId").asText();
                            // Send the entire ticker node to Kafka
                            producer.sendQuote(instId, ticker);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Error handling OKX ticker message", e);
            }
        }
    }
}
