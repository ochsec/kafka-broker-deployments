variable "linode_token" {
  description = "Linode API token"
  type        = string
  sensitive   = true
}

variable "kafka_instance_type" {
  description = "Linode plan label for the Kafka instance"
  type        = string
  default     = "g6-standard-2"
}

variable "kafka_region" {
  description = "Linode data center region"
  type        = string
  default     = "us-east"
}

variable "ssh_public_key" {
  description = "Path to the local SSH public key to inject into the instance"
  type        = string
  default     = "~/.ssh/id_ed25519.pub"
}

variable "kafka_version" {
  description = "Apache Kafka version to install"
  type        = string
  default     = "3.7.0"
}

variable "scala_version" {
  description = "Scala version bundled with the Kafka download"
  type        = string
  default     = "scala-2.13"
}