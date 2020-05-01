#!/usr/bin/env bash

#first argument of the script should be the IP/hostname
ip=$1
scp dejaq-linux.zip ec2-user@${$ip}:/home/ec2-user/
ssh ec2-user@${ip} unzip -o dejaq-linux.zip
ssh ec2-user@${ip} rm dejaq-linux.zip
ssh ec2-user@${ip} ./bin/install_utils_amazonlinux.sh
