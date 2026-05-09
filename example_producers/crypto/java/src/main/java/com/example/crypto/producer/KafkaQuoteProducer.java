package com.example.crypto.producer;

import com.example.crypto.model.QuoteEnvelope;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Wraps the Kafka producer and handles sending quote envelopes.
 */
public class KafkaQuoteProducer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaQuoteProducer.class);
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper;
    private final String topic;
    private final String exchange;

    public KafkaQuoteProducer(String bootstrapServers, String topic, String exchange) {
        this.topic = topic;
        this.exchange = exchange;
        this.mapper = new ObjectMapper();

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("client.id", "crypto-quote-producer-" + exchange);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "1");
        props.put("retries", "3");
        props.put("max.in.flight.requests.per.connection", "5");

        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Sends a quote envelope to Kafka.
     * The key is "{symbol}:{exchange}" to ensure ordering per feed.
     *
     * @param symbol the trading symbol (e.g., "BTC-USD")
     * @param rawMessage the raw JSON message from the exchange
     */
    public void sendQuote(String symbol, JsonNode rawMessage) {
        try {
            long receivedAtMs = System.currentTimeMillis();
            QuoteEnvelope envelope = new QuoteEnvelope(exchange, symbol, receivedAtMs, rawMessage);
            String envelopeJson = mapper.writeValueAsString(envelope);

            String key = symbol + ":" + exchange;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, envelopeJson);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to send quote to Kafka", exception);
                } else {
                    logger.debug("Quote sent to partition {} offset {}", 
                        metadata.partition(), metadata.offset());
                }
            });
        } catch (Exception e) {
            logger.error("Error sending quote envelope", e);
        }
    }

    /**
     * Flushes any pending messages and closes the producer.
     */
    public void close() {
        logger.info("Flushing and closing Kafka producer");
        try {
            producer.flush();
            producer.close();
        } catch (Exception e) {
            logger.error("Error closing Kafka producer", e);
        }
    }
}
