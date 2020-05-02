# --------------------------------------------
# Provider and credentials
# --------------------------------------------
provider "aws" {
  version = "~> 2.8"
  region = var.region
  secret_key = var.secret-key
  access_key = var.access-key
}

# --------------------------------------------
# Upload ssh public key on machine
# --------------------------------------------
resource "aws_key_pair" "dejaq" {
  key_name = "dejaq"
  public_key = var.public-key
  tags = {
    project = "dejaq-test"
  }
}

resource "aws_default_vpc" "default" {
  tags = {
    name = "Default VPC"
    project = "dejaq-test"
  }
}

# --------------------------------------------
# Security groups
# --------------------------------------------
resource "aws_security_group" "ssh" {
  name = "ssh"
  description = "ingress for ssh on 22, egress for all"
  tags = {
    project = "dejaq-test"
  }
  ingress {
    from_port = 22
    to_port = 22
    protocol = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
resource "aws_security_group" "metrics" {
  name = "metrics"
  description = "ingress for metrics 2110, 2111, 2112"
  tags = {
    project = "dejaq-test"
  }
  ingress {
    from_port = 2112
    to_port = 2112
    protocol = "tcp"
    cidr_blocks = [aws_default_vpc.default.cidr_block]
  }
  ingress {
    from_port = 2111
    to_port = 2111
    protocol = "tcp"
    cidr_blocks = [aws_default_vpc.default.cidr_block]
  }
  ingress {
    from_port = 2110
    to_port = 2110
    protocol = "tcp"
    cidr_blocks = [aws_default_vpc.default.cidr_block]
  }
}
resource "aws_security_group" "broker-grpc" {
  name = "broker-binding-address"
  description = "ingress on 9000 for broker grpc"
  tags = {
    project = "dejaq-test"
  }
  ingress {
    from_port = 9000
    to_port = 9000
    protocol = "tcp"
    cidr_blocks = [aws_default_vpc.default.cidr_block]
  }
}
resource "aws_security_group" "prometheus" {
  name = "prometheus"
  description = "ingress on 9090 for prometheus"
  tags = {
    project = "dejaq-test"
  }
  ingress {
    from_port = 9090
    to_port = 9090
    protocol = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --------------------------------------------
# Ec2 templates files
# --------------------------------------------
data "template_file" "broker" {
  count = var.broker-count
  template = file("user-data/broker.sh")
  vars = {
    host_name = "broker${count.index}"
  }
}
data "template_file" "producer" {
  count = var.producer-count
  template = file("user-data/producer.sh")
  vars = {
    host_name = "producer${count.index}"
  }
}
data "template_file" "consumer" {
  count = var.consumer-count
  template = file("user-data/consumer.sh")
  vars = {
    host_name = "consumer${count.index}"
  }
}
data "template_file" "prometheus" {
  template = file("user-data/prometheus.sh")
  vars = {
    host_name = "prometheus"
  }
}

# --------------------------------------------
# EC2 instances
# --------------------------------------------
resource "aws_instance" "broker" {
  count = var.broker-count
  ami = var.instance-ami
  instance_type = var.instance-type
  key_name = aws_key_pair.dejaq.key_name
  security_groups = [
    aws_security_group.ssh.name,
    aws_security_group.metrics.name,
    aws_security_group.broker-grpc.name
  ]
  availability_zone = var.availability_zone
  source_dest_check = false
  associate_public_ip_address = true
  user_data = data.template_file.broker[count.index].rendered

  tags = {
    name = "broker${count.index}"
    project = "dejaq-test"
  }
}
resource "aws_instance" "producer" {
  count = var.producer-count
  ami = var.instance-ami
  instance_type = var.instance-type
  key_name = aws_key_pair.dejaq.key_name
  security_groups = [
    aws_security_group.ssh.name,
    aws_security_group.metrics.name
  ]
  availability_zone = var.availability_zone
  source_dest_check = false
  associate_public_ip_address = true
  user_data = data.template_file.producer[count.index].rendered

  tags = {
    name = "producer${count.index}"
    project = "dejaq-test"
  }
}
resource "aws_instance" "consumer" {
  count = var.consumer-count
  ami = var.instance-ami
  instance_type = var.instance-type
  key_name = aws_key_pair.dejaq.key_name
  security_groups = [
    aws_security_group.ssh.name,
    aws_security_group.metrics.name
  ]
  availability_zone = var.availability_zone
  source_dest_check = false
  associate_public_ip_address = true
  user_data = data.template_file.consumer[count.index].rendered

  tags = {
    name = "consumer${count.index}"
    project = "dejaq-test"
  }
}
resource "aws_instance" "prometheus" {
  ami = var.instance-ami
  instance_type = var.prometheus-instance-type
  key_name = aws_key_pair.dejaq.key_name
  security_groups = [
    aws_security_group.ssh.name,
    aws_security_group.prometheus.name
  ]
  availability_zone = var.availability_zone
  source_dest_check = false
  associate_public_ip_address = true
  user_data = data.template_file.prometheus.rendered

  tags = {
    name = "prometheus"
    project = "dejaq-test"
  }
}

output "Broker-Names" {
  value = aws_instance.broker.*.tags.name
}
output "Broker-Private-Ips" {
  value = aws_instance.broker.*.private_ip
}
output "Broker-Public-Ips" {
  value = aws_instance.broker.*.public_ip
}

output "Producer-Names" {
  value = aws_instance.producer.*.tags.name
}
output "Producer-Private-Ips" {
  value = aws_instance.producer.*.private_ip
}
output "Producer-Public-Ips" {
  value = aws_instance.producer.*.public_ip
}

output "Consumer-Names" {
  value = aws_instance.consumer.*.tags.name
}
output "Consumer-Private-Ips" {
  value = aws_instance.consumer.*.private_ip
}
output "Consumer-Public-Ips" {
  value = aws_instance.consumer.*.public_ip
}

output "Prometheus-Public-Ips" {
  value = aws_instance.prometheus.public_ip
}

# --------------------------------------------
# Redis
# --------------------------------------------
resource "aws_elasticache_cluster" "redis" {
  count = 1
  cluster_id = "dejaq-test"
  engine = "redis"
  node_type = var.redis-instance-type
  num_cache_nodes = var.redis-count
  parameter_group_name = "default.redis5.0"
  engine_version = "5.0.6"
  port = 6379
  availability_zone = var.availability_zone

  tags = {
    name = "redis"
    project = "dejaq-test"
  }
}

output "Redis-Dns" {
  value = aws_elasticache_cluster.redis.*.cluster_address
}
