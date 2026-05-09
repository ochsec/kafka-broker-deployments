# Crypto Quote Kafka Producer

A Java application that streams real-time cryptocurrency quotes from OKX, Kraken, and Coinbase directly to Apache Kafka.

## Overview

This producer:
- Connects to **one exchange at a time** via WebSocket (OKX, Kraken, or Coinbase)
- Streams live ticker/quote data for a configurable crypto-USD pair
- Wraps each raw broker message with a producer-side timestamp
- Publishes timestamped quotes to a configurable Kafka topic
- Runs with **zero external dependencies** (all exchanges offer free public WebSocket APIs)

## Architecture

```
Exchange WebSocket → ExchangeClient → KafkaQuoteProducer → Kafka Topic
```

### Components

- **ExchangeClient** (interface): Abstracts exchange-specific WebSocket logic
  - `CoinbaseClient`: Coinbase Advanced Trade WebSocket
  - `KrakenClient`: Kraken WebSocket v2
  - `OkxClient`: OKX WebSocket v5 Public

- **KafkaQuoteProducer**: Wraps the Kafka producer and sends quote envelopes

- **QuoteEnvelope**: JSON-serialized message containing:
  - `exchange`: exchange name
  - `symbol`: trading pair
  - `received_at_ms`: producer-side millisecond timestamp
  - `raw`: complete, unmodified broker message

- **ProducerConfig**: Reads and validates environment variables

## Prerequisites

- **Java 11+**
- **Maven 3.6+**
- **Kafka broker** (running and accessible)

## Configuration

All configuration is via environment variables:

| Variable | Required | Description | Example |
|----------|----------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Yes | Kafka broker addresses (comma-separated) | `localhost:9092` or `192.0.2.1:9092,192.0.2.2:9092` |
| `KAFKA_TOPIC` | Yes | Topic to publish quotes to | `crypto-quotes` |
| `EXCHANGE` | Yes | Exchange to connect to | `coinbase`, `kraken`, or `okx` |
| `SYMBOL` | Yes | Trading pair (exchange-native format) | See table below |

### Supported Pairs

| Exchange | Symbol Format | Examples |
|----------|---------------|----------|
| Coinbase | `CRYPTO-USD` | `BTC-USD`, `ETH-USD` |
| Kraken | `CRYPTO/USD` | `BTC/USD`, `ETH/USD` |
| OKX | `CRYPTO-USDC` or `CRYPTO-USDT` | `BTC-USDC`, `ETH-USDT` |

## Build

```bash
cd example_producers/crypto/java
mvn clean package
```

This creates a fat JAR at `target/crypto-quote-producer.jar` with all dependencies included.

## Usage

### Example 1: Stream Coinbase BTC-USD to local Kafka

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=crypto-quotes
export EXCHANGE=coinbase
export SYMBOL=BTC-USD

java -jar target/crypto-quote-producer.jar
```

### Example 2: Stream Kraken BTC/USD

```bash
export KAFKA_BOOTSTRAP_SERVERS=kafka-broker.example.com:9092
export KAFKA_TOPIC=btc-usd-quotes
export EXCHANGE=kraken
export SYMBOL=BTC/USD

java -jar target/crypto-quote-producer.jar
```

### Example 3: Stream OKX BTC-USDC

```bash
export KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092,kafka3:9092
export KAFKA_TOPIC=okx-btc-usdc
export EXCHANGE=okx
export SYMBOL=BTC-USDC

java -jar target/crypto-quote-producer.jar
```

## Kafka Message Format

### Message Key
```
{symbol}:{exchange}
```
Example: `BTC-USD:coinbase`, `BTC/USD:kraken`, `BTC-USDC:okx`

This ensures ordered consumption per symbol/exchange pair.

### Message Value (JSON)
```json
{
  "exchange": "coinbase",
  "symbol": "BTC-USD",
  "received_at_ms": 1746800000123,
  "raw": {
    "type": "ticker",
    "product_id": "BTC-USD",
    "price": "42500.50",
    "volume_24_h": "25000.12345678",
    "best_bid": "42500.25",
    "best_bid_quantity": "1.5",
    "best_ask": "42500.75",
    "best_ask_quantity": "2.0",
    ...
  }
}
```

- `received_at_ms`: Wall-clock timestamp (milliseconds since epoch) when the producer received the message from the exchange
- `raw`: The complete, unmodified JSON message from the exchange broker

## Consuming Messages

### Using kafka-console-consumer

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic crypto-quotes \
  --from-beginning | jq '.' # pipe to jq for pretty-printing
```

### Inspecting the raw broker message
```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic crypto-quotes \
  --value-deserializer org.apache.kafka.common.serialization.StringDeserializer \
  --max-messages 1 | jq '.raw'
```

## Exchange-Specific Notes

### Coinbase
- **Public API**: Yes, no API key required
- **Pair format**: `BTC-USD`, `ETH-USD`
- **Subscription**: Automatically subscribes to `ticker` and `heartbeats` channels
- **Heartbeats**: Sent every ~1 second to keep connection alive (idle timeout: 60–90 s)
- **Update frequency**: Per trade match

### Kraken
- **Public API**: Yes, no API key required
- **Pair format**: `BTC/USD`, `ETH/USD`
- **Subscription**: Subscribes to `ticker` channel
- **Heartbeats**: Sent automatically by server
- **Update frequency**: Per trade event
- **Reconnect on**: >35 seconds without a heartbeat

### OKX
- **Public API**: Yes, no API key required
- **Pair format**: `BTC-USDC` (spot), `BTC-USDT` (spot), `BTC-USD-SWAP` (perp)
- **Subscription**: Subscribes to `tickers` channel
- **Ping/Pong**: Client must send text `ping` every ~25 seconds; server responds with text `pong`
- **Update frequency**: Per trade or BBO change (≥100ms)

## Error Handling & Reconnection

Each exchange client implements automatic reconnection with exponential backoff:

1. **Initial backoff**: 1 second
2. **Progressive backoff**: Doubles on each retry (1s → 2s → 4s → ... → 30s cap)
3. **Trigger**: WebSocket disconnection, message timeout, or other errors

The producer logs all errors and reconnection attempts at the WARN level.

## Logging

The producer uses SLF4J with SimpleLogger. By default, logs are printed to stdout at INFO level.

To increase verbosity:
```bash
export SLF4J_DEFAULT_LOG_LEVEL=DEBUG
java -jar target/crypto-quote-producer.jar
```

To reduce verbosity:
```bash
export SLF4J_DEFAULT_LOG_LEVEL=WARN
java -jar target/crypto-quote-producer.jar
```

## Running in Docker

### Build a Docker image

Create a `Dockerfile`:

```dockerfile
FROM maven:3.9-eclipse-temurin-11 as builder
WORKDIR /build
COPY . .
RUN mvn clean package -DskipTests

FROM eclipse-temurin:11-jre-jammy
WORKDIR /app
COPY --from=builder /build/target/crypto-quote-producer.jar .
ENTRYPOINT ["java", "-jar", "crypto-quote-producer.jar"]
```

Build and run:

```bash
docker build -t crypto-quote-producer .

docker run -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
           -e KAFKA_TOPIC=crypto-quotes \
           -e EXCHANGE=coinbase \
           -e SYMBOL=BTC-USD \
           crypto-quote-producer
```

## Project Structure

```
example_producers/crypto/java/
├── pom.xml
└── src/
    └── main/
        └── java/
            └── com/example/crypto/
                ├── Main.java
                ├── config/
                │   └── ProducerConfig.java
                ├── producer/
                │   └── KafkaQuoteProducer.java
                ├── exchange/
                │   ├── ExchangeClient.java
                │   ├── CoinbaseClient.java
                │   ├── KrakenClient.java
                │   └── OkxClient.java
                └── model/
                    └── QuoteEnvelope.java
```

## Dependencies

- **kafka-clients:3.7.0**: Apache Kafka client library
- **Java-WebSocket:1.5.6**: WebSocket client library
- **jackson-databind:2.16.0**: JSON serialization/deserialization
- **slf4j-api & slf4j-simple:2.0.9**: Logging framework

## Development

To modify or extend the producer:

1. **Add a new exchange**: Create a new `src/main/java/com/example/crypto/exchange/NewExchangeClient.java` implementing `ExchangeClient`
2. **Customize message format**: Modify `QuoteEnvelope` and `KafkaQuoteProducer.sendQuote()`
3. **Add authentication**: Extend `ProducerConfig` to read API credentials from env vars

## Graceful Shutdown

The producer responds to OS signals (`SIGTERM`, `SIGINT`):

```bash
# Send Ctrl+C to shut down gracefully
java -jar target/crypto-quote-producer.jar
# ^ Press Ctrl+C

# Or from another terminal:
kill -TERM <PID>
```

On shutdown, the producer:
1. Stops accepting new messages from the exchange
2. Flushes all pending Kafka messages
3. Closes all connections gracefully

## Troubleshooting

### "Environment variable 'KAFKA_BOOTSTRAP_SERVERS' is required but not set"
Ensure all four required env vars are exported:
```bash
export KAFKA_BOOTSTRAP_SERVERS=...
export KAFKA_TOPIC=...
export EXCHANGE=...
export SYMBOL=...
```

### "Connection refused" or WebSocket timeout
- Verify the Kafka broker is running and accessible at the bootstrap server address
- Check network connectivity to the exchange WebSocket endpoints
- Verify firewall rules allow outbound connections to the exchanges

### Messages not appearing in Kafka
- Verify the topic exists: `kafka-topics.sh --list --bootstrap-server <broker>`
- Check producer logs for errors (increase log level to DEBUG)
- Verify Kafka broker is accepting connections

### High latency
- Kafka producer batching may cause slight latency (default: 16 KB or 100 ms, whichever comes first)
- Disable batching in `KafkaQuoteProducer.java` by setting `linger.ms=0` if needed

## License

This project is provided as-is for use with the Kafka broker deployment infrastructure.
