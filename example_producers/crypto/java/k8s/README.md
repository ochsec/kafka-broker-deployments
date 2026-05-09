# Kubernetes Deployment for Crypto Quote Producers

This directory contains cloud-agnostic Kubernetes manifests for orchestrating multiple crypto quote producer instances.

## Architecture

The deployment uses a **controller-based approach** where:

1. **A Python controller Job** reads a list of producer configurations from a ConfigMap
2. **For each producer entry**, the controller creates a Kubernetes Pod with the appropriate environment variables
3. **Pods run independently** with `restartPolicy: Always`, so Kubernetes automatically restarts failed producers
4. **Idempotent add-only strategy**: Re-running the controller only creates missing pods, does not delete or modify existing ones
5. **Kafka broker address** is injected via Kubernetes Secret (supports different brokers per producer)

## Files

- **namespace.yaml** — Dedicated namespace `crypto-producers`
- **rbac.yaml** — ServiceAccount, Role, and RoleBinding for the controller
- **secret.yaml** — Template Secret containing `KAFKA_BOOTSTRAP_SERVERS` (edit before applying)
- **producers-configmap.yaml** — ConfigMap listing all (exchange, symbol, topic, broker) tuples
- **controller-configmap.yaml** — Python controller script stored as ConfigMap data
- **controller-job.yaml** — Kubernetes Job that runs the controller once to spawn/sync producer pods

## Quick Start

### 1. Create the namespace and RBAC

```bash
kubectl apply -f namespace.yaml
kubectl apply -f rbac.yaml
```

### 2. Configure the Kafka broker Secret

Edit `secret.yaml` and replace `YOUR_BROKER_HOST:9092` with the actual broker address(es):

```bash
# Example: single broker
KAFKA_BOOTSTRAP_SERVERS: "kafka.example.com:9092"

# Example: cluster of brokers
KAFKA_BOOTSTRAP_SERVERS: "broker1:9092,broker2:9092,broker3:9092"
```

Then apply:

```bash
kubectl apply -f secret.yaml
```

### 3. Configure producers

Edit `producers-configmap.yaml` to add, remove, or modify producer entries:

```json
[
  {
    "exchange": "coinbase",
    "symbol": "BTC-USD",
    "topic": "crypto-quotes",
    "secretRef": "kafka-broker-secret"
  },
  ...
]
```

Then apply:

```bash
kubectl apply -f producers-configmap.yaml
```

### 4. Create the controller ConfigMap

```bash
kubectl apply -f controller-configmap.yaml
```

### 5. Run the controller Job

```bash
kubectl apply -f controller-job.yaml
```

This creates a Job that reads the producers list and spawns one Pod per entry.

## Viewing the Results

### Check Job completion

```bash
kubectl get jobs -n crypto-producers
kubectl logs -n crypto-producers job/producer-controller
```

### List all producer pods

```bash
kubectl get pods -n crypto-producers -l app=crypto-producer
```

### View logs from a specific producer

```bash
kubectl logs -n crypto-producers crypto-producer-coinbase-btc-usd
```

### Describe a producer pod (see env vars, errors, etc.)

```bash
kubectl describe pod -n crypto-producers crypto-producer-coinbase-btc-usd
```

## Adding / Removing Producers

### To add a new producer:

1. Edit `producers-configmap.yaml` and add a new entry to the JSON array
2. Apply the updated ConfigMap:
   ```bash
   kubectl apply -f producers-configmap.yaml
   ```
3. Delete and re-run the controller Job:
   ```bash
   kubectl delete job producer-controller -n crypto-producers
   kubectl apply -f controller-job.yaml
   ```
4. The controller will create the new Pod (existing pods are unchanged)

### To remove a producer:

1. Edit `producers-configmap.yaml` and remove the entry
2. Apply the updated ConfigMap:
   ```bash
   kubectl apply -f producers-configmap.yaml
   ```
3. Manually delete the Pod:
   ```bash
   kubectl delete pod crypto-producer-exchange-symbol -n crypto-producers
   ```
   (The controller does not delete pods, so manual cleanup is required)

## Monitoring and Debugging

### Check if a producer is running

```bash
kubectl get pod -n crypto-producers crypto-producer-coinbase-btc-usd
```

If `STATUS` is `Running` and `RESTARTS` is low, the producer is healthy.

### Debug a failing producer

```bash
# View recent logs
kubectl logs -n crypto-producers crypto-producer-coinbase-btc-usd --tail=50

# View previous logs (if the pod has restarted)
kubectl logs -n crypto-producers crypto-producer-coinbase-btc-usd --previous

# Get detailed info including events and errors
kubectl describe pod -n crypto-producers crypto-producer-coinbase-btc-usd
```

Common issues:
- **ImagePullBackOff**: Image `ghcr.io/replmade/crypto-quote-producer:latest` is not available. Push the image to the registry first.
- **CrashLoopBackOff**: Producer app is crashing. Check logs for errors (usually missing env vars or invalid Kafka broker address).
- **Pending**: Pod cannot find a node to run on. Check cluster resources and node labels.

### Check controller logs

```bash
kubectl logs -n crypto-producers job/producer-controller
```

Output shows how many pods were created vs. skipped.

## Pod Naming Convention

Each producer pod is named: `crypto-producer-{exchange}-{symbol}`

Examples:
- Coinbase BTC-USD → `crypto-producer-coinbase-btc-usd`
- Kraken BTC/USD → `crypto-producer-kraken-btc-usd`
- OKX BTC-USDC → `crypto-producer-okx-btc-usdc`

(Symbols are normalized: `/` → `-`, uppercase → lowercase)

## Environment Variables Injected

Each producer pod receives:

| Variable | Source |
|---|---|
| `EXCHANGE` | From ConfigMap (e.g., `coinbase`) |
| `SYMBOL` | From ConfigMap (e.g., `BTC-USD`) |
| `KAFKA_TOPIC` | From ConfigMap (e.g., `crypto-quotes`) |
| `KAFKA_BOOTSTRAP_SERVERS` | From Secret, via `secretKeyRef` |

## Resource Limits

Each producer pod is configured with:

- **Request**: 256 MB memory, 100m CPU
- **Limit**: 512 MB memory, 500m CPU

Adjust in `controller-job.yaml` under `pod_spec["spec"]["containers"][0]["resources"]` if needed.

## Multiple Brokers

To support multiple Kafka brokers (e.g., cluster with failover), edit `secret.yaml`:

```yaml
stringData:
  KAFKA_BOOTSTRAP_SERVERS: "broker1.internal:9092,broker2.internal:9092,broker3.internal:9092"
```

All producers sharing the same `secretRef` will connect to the broker cluster.

To use **different broker clusters** for different producers, create multiple Secrets and reference them in `producers-configmap.yaml`:

```json
[
  {
    "exchange": "coinbase",
    "symbol": "BTC-USD",
    "topic": "quotes-cluster-a",
    "secretRef": "kafka-broker-secret-a"
  },
  {
    "exchange": "kraken",
    "symbol": "BTC/USD",
    "topic": "quotes-cluster-b",
    "secretRef": "kafka-broker-secret-b"
  }
]
```

## Clean Up

To remove all producer resources:

```bash
kubectl delete job producer-controller -n crypto-producers
kubectl delete pods -l app=crypto-producer -n crypto-producers
kubectl delete configmap controller-script producers-config -n crypto-producers
kubectl delete secret kafka-broker-secret -n crypto-producers
kubectl delete sa producer-controller -n crypto-producers
kubectl delete role producer-controller -n crypto-producers
kubectl delete rolebinding producer-controller -n crypto-producers
kubectl delete namespace crypto-producers
```

Or simply:

```bash
kubectl delete namespace crypto-producers
```

(Deleting the namespace removes all contained resources)

## Future Enhancements

- **StatefulSet** instead of raw Pods for better management
- **Helm chart** for templating (values.yaml can define producers list)
- **Observability**: Prometheus metrics export from the Java producer
- **Graceful shutdown**: Handle SIGTERM to flush in-flight messages
- **ConfigMap watch**: Controller could run as a continuous deployment and watch for ConfigMap changes instead of being a one-shot Job
