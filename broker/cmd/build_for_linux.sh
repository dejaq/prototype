#!/usr/bin/env bash

export GOOS=linux GOARCH=amd64

mkdir bin
go build -o bin/dejaqcli-broker dejaqcli-broker/main.go
go build -o bin/dejaqcli-producer dejaqcli-producer/main.go
go build -o bin/dejaqcli-consumer dejaqcli-consumer/main.go
cp start-all-local/* bin/

zip dejaq-linux bin/*
rm bin/*

#TODO get the hosts generted by terraform
# cat hosts.txt | xargs -I HOST scp dejaq-linux.zip HOST
#these presumes you have a custom .ssh/config that applies your AWS key
ip=ec2-35-181-7-101.eu-west-3.compute.amazonaws.com
scp dejaq-linux.zip ${ip}:/home/ec2-user/
ssh ${ip} unzip -o dejaq-linux.zip
ssh ${ip} rm dejaq-linux.zip
rm dejaq-linux.zip

ssh ${ip} ./bin/install_utils_amazonlinux.sh
