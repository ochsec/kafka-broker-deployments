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