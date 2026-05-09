# Quick Start Guide: Crypto Quote Producer

## Prerequisites
- Java 11+ installed
- Maven 3.6+ installed
- Access to a Kafka broker

## 1. Build the Producer

From the `example_producers/crypto/java` directory:

```bash
mvn clean package
```

This creates `target/crypto-quote-producer.jar`.

## 2. Set Environment Variables

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=crypto-quotes
export EXCHANGE=coinbase
export SYMBOL=BTC-USD
```

## 3. Create the Kafka Topic (if needed)

Using a Kafka broker running locally:

```bash
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic crypto-quotes \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists
```

## 4. Run the Producer

```bash
java -jar target/crypto-quote-producer.jar
```

You should see logs like:
```
INFO - Configuration loaded: ProducerConfig{bootstrapServers='localhost:9092', topic='crypto-quotes', exchange='coinbase', symbol='BTC-USD'}
INFO - Kafka producer initialized
INFO - Exchange client initialized for coinbase: BTC-USD
INFO - Starting to stream quotes from coinbase to topic crypto-quotes
INFO - Coinbase WebSocket opened
INFO - Subscribed to ticker for BTC-USD
INFO - Subscribed to heartbeats
```

## 5. Consume Messages (in another terminal)

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic crypto-quotes \
  --from-beginning \
  --property print.key=true
```

Sample output:
```
BTC-USD:coinbase	{"exchange":"coinbase","symbol":"BTC-USD","received_at_ms":1746800123456,"raw":{"type":"ticker","product_id":"BTC-USD","price":"42500.50",...}}
```

## Docker Compose Quick Start

If you want to run everything in Docker (Kafka + Producer):

```bash
# Build the producer image
docker build -t crypto-quote-producer .

# Start Kafka and producer
docker-compose up

# In another terminal, view messages:
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic crypto-quotes \
  --from-beginning
```

## Switching Exchanges

To stream from a different exchange, set the environment variables and restart:

```bash
# Kraken BTC/USD
export EXCHANGE=kraken
export SYMBOL=BTC/USD
java -jar target/crypto-quote-producer.jar

# OKX BTC-USDC
export EXCHANGE=okx
export SYMBOL=BTC-USDC
java -jar target/crypto-quote-producer.jar
```

## Troubleshooting

**"Connection refused"**: Make sure your Kafka broker is running and listening on the bootstrap server address.

**"No env vars set"**: All four required env vars must be set:
- KAFKA_BOOTSTRAP_SERVERS
- KAFKA_TOPIC
- EXCHANGE
- SYMBOL

**"WebSocket timeout"**: Check network connectivity to the exchange. Some exchanges may rate-limit or block certain regions.

For more details, see `README.md`.
