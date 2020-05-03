#!/usr/bin/env bash

cd "$(dirname "$0")"
current_dir=$(pwd)


#populate the IP files
echo "fetching the IPs ..."
cd ./terraform/
#this way the clients will know how to reach the broker
terraform output -json Broker-Private-Ips | jq -c -r  '.[0]' > ${current_dir}/bin/broker.privateip
#terraform output -json Redis-Private-Ips | jq -c -rr  '.[0]' > ${current_dir}/bin/redis.privateip

broker_public_ip=$(terraform output -json Broker-Public-Ips | jq -c -r  '.[0]')
#TODO this will not work with multiple IPs modify the jq
producer_public_ip=$(terraform output -json Producer-Public-Ips | jq -c -r  '.[0]')
consumer_public_ip=$(terraform output -json Consumer-Public-Ips | jq -c -r  '.[0]')

cd ${current_dir}

echo "starting the brokers, consumer and producer"
#start_broker_redis.sh OR start_broker.sh
ssh ec2-user@${broker_public_ip} './bin/start_broker_redis.sh >out.log 2>err.log &'
ssh ec2-user@${producer_public_ip} './bin/start_producer.sh >out.log 2>err.log &'
ssh ec2-user@${consumer_public_ip} './bin/start_consumer.sh >out.log 2>err.log &'

echo "multitail -l 'ssh ec2-user@${broker_public_ip} tail -f *.log' -l 'ssh ec2-user@${producer_public_ip} tail -f *.log' -l 'ssh ec2-user@${consumer_public_ip} tail -f *.log' "