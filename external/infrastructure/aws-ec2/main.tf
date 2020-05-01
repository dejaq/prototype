# --------------------------------------------
# Provider and credentials
# --------------------------------------------
provider "aws" {
  version = "~> 2.0"
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
    Name = "Default VPC"
  }
}

# --------------------------------------------
# Security groups
# --------------------------------------------
resource "aws_security_group" "ssh-metrics" {
  name = "ssh-metrics"
  description = "allow ssh from outside and metrics"
  ingress {
    from_port = 22
    to_port = 22
    protocol = "tcp"
    cidr_blocks = [
      "0.0.0.0/0"]
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
  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = [
      "0.0.0.0/0"]
  }
}

resource "aws_security_group" "broker-grpc" {
  name = "broker-binding-address"
  description = "allow access to app"
  ingress {
    from_port = 9000
    to_port = 9000
    protocol = "tcp"
    cidr_blocks = [aws_default_vpc.default.cidr_block]
  }
}

# --------------------------------------------
# Instaces templates files
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

# --------------------------------------------
# EC2 instances
# --------------------------------------------
resource "aws_instance" "broker" {
  count = var.broker-count
  ami = var.instance-ami
  instance_type = var.instance-type
  key_name = aws_key_pair.dejaq.key_name
  security_groups = [aws_security_group.ssh-metrics.name, aws_security_group.broker-grpc.name]
  source_dest_check = false
  associate_public_ip_address = true
  user_data = data.template_file.broker[count.index].rendered

  tags = {
    name = "broker${count.index}"
    project-dejaq = "dejaq-test"
  }
}

resource "aws_instance" "producer" {
  count = var.producer-count
  ami = var.instance-ami
  instance_type = var.instance-type
  key_name = aws_key_pair.dejaq.key_name
  security_groups = [aws_security_group.ssh-metrics.name]
  source_dest_check = false
  associate_public_ip_address = true
  user_data = data.template_file.producer[count.index].rendered

  tags = {
    name = "producer${count.index}"
    project-dejaq = "dejaq-test"
  }
}

resource "aws_instance" "consumer" {
  count = var.consumer-count
  ami = var.instance-ami
  instance_type = var.instance-type
  key_name = aws_key_pair.dejaq.key_name
  security_groups = [aws_security_group.ssh-metrics.name]
  source_dest_check = false
  associate_public_ip_address = true
  user_data = data.template_file.consumer[count.index].rendered

  tags = {
    name = "consumer${count.index}"
    project-dejaq = "dejaq-test"
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