package com.example.crypto;

import com.example.crypto.config.ProducerConfig;
import com.example.crypto.exchange.CoinbaseClient;
import com.example.crypto.exchange.ExchangeClient;
import com.example.crypto.exchange.KrakenClient;
import com.example.crypto.exchange.OkxClient;
import com.example.crypto.producer.KafkaQuoteProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for the Crypto Quote Kafka Producer.
 * Reads configuration from environment variables, initializes the appropriate
 * exchange client, and starts streaming quotes to Kafka.
 */
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        ExchangeClient exchangeClient = null;
        KafkaQuoteProducer kafkaProducer = null;

        try {
            // Load configuration from environment
            ProducerConfig config = ProducerConfig.fromEnv();
            logger.info("Configuration loaded: {}", config);

            // Initialize Kafka producer
            kafkaProducer = new KafkaQuoteProducer(
                config.getBootstrapServers(),
                config.getTopic(),
                config.getExchange()
            );
            logger.info("Kafka producer initialized");

            // Initialize the appropriate exchange client
            String exchange = config.getExchange();
            String symbol = config.getSymbol();

            exchangeClient = switch (exchange) {
                case "coinbase" -> new CoinbaseClient(symbol);
                case "kraken" -> new KrakenClient(symbol);
                case "okx" -> new OkxClient(symbol);
                default -> throw new IllegalArgumentException("Unknown exchange: " + exchange);
            };
            logger.info("Exchange client initialized for {}: {}", exchange, symbol);

            // Setup graceful shutdown hook
            KafkaQuoteProducer finalKafkaProducer = kafkaProducer;
            ExchangeClient finalExchangeClient = exchangeClient;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown signal received");
                finalExchangeClient.shutdown();
                finalKafkaProducer.close();
                logger.info("Shutdown complete");
            }));

            // Start streaming quotes
            logger.info("Starting to stream quotes from {} to topic {}",
                exchange, config.getTopic());
            exchangeClient.connect(kafkaProducer);

        } catch (Exception e) {
            logger.error("Fatal error in producer", e);
            if (exchangeClient != null) {
                exchangeClient.shutdown();
            }
            if (kafkaProducer != null) {
                kafkaProducer.close();
            }
            System.exit(1);
        }
    }
}
