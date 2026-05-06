# Setting Up a Kafka Broker on Linode Using Terraform

This guide walks you through provisioning a Linode instance and deploying an Apache Kafka broker on it using Docker and KRaft, entirely through infrastructure-as-code with Terraform.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Project Structure](#2-project-structure)
3. [Terraform Configuration](#3-terraform-configuration)
   - [Provider Configuration](#31-provider-configuration)
   - [Variables](#32-variables)
   - [SSH Key Resource](#33-ssh-key-resource)
   - [Linode Instance Resource](#34-linode-instance-resource)
   - [Cloud-Init / User Data](#35-cloud-init--user-data)
4. [Kafka Installation (Cloud-Init / Docker)](#4-kafka-installation-cloud-init--docker)
5. [Deploying with Terraform](#5-deploying-with-terraform)
6. [Verifying the Deployment](#6-verifying-the-deployment)
7. [Producing and Consuming Messages](#7-producing-and-consuming-messages)
8. [Cleaning Up / Destroying Resources](#8-cleaning-up--destroying-resources)
9. [Troubleshooting](#9-troubleshooting)
10. [Next Steps](#10-next-steps)

---

## 1. Prerequisites

| Requirement | Details |
|---|---|
| **Linode account** | Sign up at [cloud.linode.com](https://cloud.linode.com/) |
| **Linode API token** | Generate at _Account → API Tokens_ (must have read/write scope) |
| **Terraform ≥ 1.5** | Install from [developer.hashicorp.com](https://developer.hashicorp.com/terraform/downloads) |
| **SSH key pair** | Used for authenticating into the instance |
| **Local SSH client** | For connecting to the provisioned instance |

### 1.1 Set the Linode API Token as an Environment Variable

```bash
export TF_VAR_linode_token="your-linode-api-token-here"
```

> **Never hardcode the token in `.tf` files.** The variable above is automatically picked up by Terraform's `variable "linode_token"` block.

---

## 2. Project Structure

Create a working directory for the project:

```
kafka-linode/
├── main.tf          # Provider, SSH key, and Linode instance
├── variables.tf     # Variable declarations
├── outputs.tf       # Useful outputs (IP address, etc.)
├── cloud-init.yaml  # Bootstrap script — installs Docker, pulls Kafka image
└── terraform.tfvars # (Optional) Default variable values
```

---

## 3. Terraform Configuration

### 3.1 Provider Configuration

**`main.tf`** — start with the Linode provider:

```hcl
terraform {
  required_providers {
    linode = {
      source  = "linode/linode"
      version = "~> 2.0"
    }
  }
}

provider "linode" {
  token = var.linode_token
}
```

### 3.2 Variables

**`variables.tf`**:

```hcl
variable "linode_token" {
  description = "Linode API token"
  type        = string
  sensitive   = true
}

variable "kafka_instance_type" {
  description = "Linode plan label for the Kafka instance"
  type        = string
  default     = "g6-standard-2"   # 2 vCPU, 4 GB RAM — good for a single-broker lab
}

variable "kafka_region" {
  description = "Linode data center region"
  type        = string
  default     = "us-east"         # Closest to most US-based users
}

variable "ssh_public_key" {
  description = "Path to the local SSH public key to inject into the instance"
  type        = string
  default     = "~/.ssh/id_ed25519.pub"
}

variable "kafka_version" {
  description = "Apache Kafka version to run (Docker image tag)"
  type        = string
  default     = "3.7.0"
}
```

### 3.3 SSH Key Resource

**`main.tf`** — add the SSH key so you can log in:

```hcl
resource "linode_sshkey" "kafka_key" {
  label   = "kafka-linode-key"
  ssh_key = trimspace(file(var.ssh_public_key))
}
```

### 3.4 Linode Instance Resource

**`main.tf`** — the compute instance:

```hcl
resource "linode_instance" "kafka" {
  label           = "kafka-broker"
  image           = "linode/ubuntu22.04"
  region          = var.kafka_region
  type            = var.kafka_instance_type
  authorized_keys = [linode_sshkey.kafka_key.ssh_key]
  root_pass       = random_password.root_pass.result

  # Cloud-init user data (see Section 4)
  metadata {
    user_data = filebase64("cloud-init.yaml")
  }

  tags = ["kafka", "terraform"]
}

resource "random_password" "root_pass" {
  length  = 32
  special = false
}
```

### 3.5 Outputs

**`outputs.tf`**:

```hcl
output "kafka_instance_ip" {
  description = "Public IP of the Kafka Linode"
  value       = tolist(linode_instance.kafka.ipv4)[0]
}

output "kafka_instance_label" {
  description = "Label of the Kafka Linode"
  value       = linode_instance.kafka.label
}
```

---

## 4. Kafka Installation (Cloud-Init / Docker)

Create **`cloud-init.yaml`** in the project root. This script runs on first boot — it installs Docker, pulls the official Apache Kafka container image, and starts Kafka in KRaft mode via a systemd unit that manages the Docker container.

```yaml
#cloud-config

package_update: true
package_upgrade: true

packages:
  - ca-certificates
  - curl
  - gnupg

write_files:
  # --- Systemd unit for Kafka (Docker) ---
  - path: /etc/systemd/system/kafka.service
    owner: root:root
    permissions: "0644"
    content: |
      [Unit]
      Description=Apache Kafka Broker (Docker)
      After=docker.service
      Requires=docker.service

      [Service]
      Type=simple
      ExecStartPre=-/usr/bin/docker stop kafka
      ExecStartPre=-/usr/bin/docker rm kafka
      ExecStart=/usr/bin/docker run --name kafka \
        -p 9092:9092 \
        -e KAFKA_NODE_ID=1 \
        -e KAFKA_PROCESS_ROLES=broker,controller \
        -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
        -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://%H:9092 \
        -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
        -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
        -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
        -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
        -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
        -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
        -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
        apache/kafka:3.7.0
      ExecStop=/usr/bin/docker stop kafka
      Restart=on-failure
      RestartSec=5

      [Install]
      WantedBy=multi-user.target

runcmd:
  # 1. Install Docker
  - |
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
      > /etc/apt/sources.list.d/docker.list
    apt-get update -y
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
    systemctl enable --now docker

  # 2. Enable and start Kafka
  - systemctl daemon-reload
  - systemctl enable --now kafka
```

> **Why KRaft?** Starting with Kafka 3.3+, the _KRaft_ (Kafka Raft) consensus protocol is production-ready and replaces the need for a separate ZooKeeper ensemble. The container runs in KRaft mode using environment variables (`KAFKA_PROCESS_ROLES=broker,controller`, `KAFKA_CONTROLLER_QUORUM_VOTERS`, etc.) — no ZooKeeper container is needed.

---

## 5. Deploying with Terraform

### 5.1 Initialize Terraform

```bash
cd kafka-linode/
terraform init
```

Expected output:

```
Initializing the backend...
Initializing provider plugins...
- Installing linode/linode v2.x.x...
- Installing hashicorp/random v3.x.x...

Terraform has been successfully initialized!
```

### 5.2 Review the Plan

```bash
terraform plan
```

Inspect the output to make sure:
- One `linode_instance` will be created.
- The `user_data` (cloud-init) is included.
- The plan makes sense for your account quotas.

### 5.3 Apply the Configuration

```bash
terraform apply -auto-approve
```

After a few minutes you will see:

```
Apply complete! Resources: 3 added, 0 changed, 0 destroyed.

Outputs:

  kafka_instance_ip = "203.0.113.42"
```

> **Note:** The cloud-init script takes roughly 2–4 minutes to finish. SSH in only after that window.

---

## 6. Verifying the Deployment

### 6.1 SSH into the Instance

Replace the IP with the output from Terraform:

```bash
ssh root@$(terraform output -raw kafka_instance_ip)
```

### 6.2 Check Kafka Service Status

```bash
systemctl status kafka
```

You should see `active (running)`.

### 6.3 Verify the Docker Container

```bash
docker ps
```

You should see the `apache/kafka` container running.

### 6.4 Verify Kafka Is Listening

```bash
ss -tlnp | grep 9092
```

Expected output (something like):

```
LISTEN  0  50  0.0.0.0:9092  0.0.0.0:*  users:(("java",pid=12345,fd=...))
```

---

## 7. Producing and Consuming Messages

All commands below assume you're SSH'd into the Linode instance.

### 7.1 Create a Topic

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic test-topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### 7.2 Produce Messages

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --topic test-topic \
  --bootstrap-server localhost:9092
```

Type some messages and press `Ctrl+C` when done.

### 7.3 Consume Messages

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic test-topic \
  --from-beginning \
  --bootstrap-server localhost:9092
```

You should see the messages you produced.

---

## 8. Cleaning Up / Destroying Resources

When you are done experimenting, tear down everything to avoid charges:

```bash
terraform destroy -auto-approve
```

This deletes the Linode instance, the SSH key, and all associated resources.

---

## 9. Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| `terraform apply` fails with 401 | Invalid or expired API token | Regenerate token and re-export `TF_VAR_linode_token` |
| SSH connection refused | Cloud-init still running | Wait 2–4 minutes, then retry |
| Kafka won't start | Docker not installed or image not pulled | Check `systemctl status docker` and `docker pull apache/kafka:3.7.0` |
| `docker: not found` | Cloud-init failed to add the Docker repo | Check `/var/log/cloud-init-output.log` for apt errors |
| `java.net.UnknownHostException` in logs | `advertised.listeners` uses hostname that doesn't resolve | The `%H` placeholder in the systemd unit resolves to the system hostname; ensure it matches the public IP or add a hosts entry |
| Client can't connect from outside Linode | Firewall / security group blocking port 9092 | Open port 9092 on the Linode or add a Linode Firewall allowing TCP/9092 |
| Insufficient memory | Plan too small (`g6-nanode-1`) | Switch to at least `g6-standard-2` (4 GB RAM) |

### 9.1 Viewing Cloud-Init Logs

```bash
cat /var/log/cloud-init-output.log
cat /var/log/cloud-init.log
```

### 9.2 Viewing Kafka Logs

```bash
journalctl -u kafka -n 100 --no-pager
```

Or view the container logs directly:

```bash
docker logs kafka --tail 100
```

---

## 10. Next Steps

- **Add a Linode Firewall** — create a `linode_firewall` resource to restrict access to ports 22 and 9092.
- **Multi-broker cluster** — use `count` or a `for_each` to spin up 3+ brokers and configure KRaft environment variables for each.
- **TLS encryption** — mount certificates into the container and configure `ssl.keystore.location` / `ssl.truststore.location` via environment variables.
- **SASL authentication** — enable `sasl.enabled.mechanisms` for authentication between clients and the broker.
- **Monitoring** — deploy JMX Exporter alongside Kafka and send metrics to Prometheus + Grafana.
- **Kafka Connect** — add a Connect worker container to stream data in/out of Kafka from external systems.
- **Docker Compose** — replace the systemd unit with a `docker-compose.yml` for easier multi-container orchestration.
- **Remote state** — store `terraform.tfstate` in an S3-compatible backend (Linode Object Storage) for team collaboration.

---

## Quick Reference — Full `main.tf`

```hcl
terraform {
  required_providers {
    linode = {
      source  = "linode/linode"
      version = "~> 2.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

provider "linode" {
  token = var.linode_token
}

resource "linode_sshkey" "kafka_key" {
  label   = "kafka-linode-key"
  ssh_key = trimspace(file(var.ssh_public_key))
}

resource "random_password" "root_pass" {
  length  = 32
  special = false
}

resource "linode_instance" "kafka" {
  label           = "kafka-broker"
  image           = "linode/ubuntu22.04"
  region          = var.kafka_region
  type            = var.kafka_instance_type
  authorized_keys = [linode_sshkey.kafka_key.ssh_key]
  root_pass       = random_password.root_pass.result

  metadata {
    user_data = filebase64("cloud-init.yaml")
  }

  tags = ["kafka", "terraform"]
}
```

---

*Guide authored for the Data Engineering workspace — May 2026.*