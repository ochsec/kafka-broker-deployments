output "kafka_instance_ip" {
  description = "Public IP of the Kafka Linode"
  value       = tolist(linode_instance.kafka.ipv4)[0]
}

output "kafka_instance_label" {
  description = "Label of the Kafka Linode"
  value       = linode_instance.kafka.label
}