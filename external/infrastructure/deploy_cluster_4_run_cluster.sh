#!/usr/bin/env bash

cd "$(dirname "$0")"
current_dir=$(pwd)

# first argument may be redis or cockroach
broker_script="start_broker.sh"
if [[ $1 == "redis" ]]; then
    broker_script="start_broker_redis.sh"
fi
if [[ $1 == "cockroach" ]]; then
    broker_script="start_broker_crdb.sh"
fi


#populate the IP files
echo "fetching the IPs ..."
cd ./terraform/
broker_public_ip=$(terraform output -json Broker-Public-Ips | jq -c -r  '.[0]')
#TODO this will not work with multiple IPs modify the jq
producer_public_ip=$(terraform output -json Producer-Public-Ips | jq -c -r  '.[]')
consumer_public_ip=$(terraform output -json Consumer-Public-Ips | jq -c -r  '.[]')
cd ${current_dir}

totail="multitail "

echo "starting the brokers, consumer and producer"
#start_broker_redis.sh OR start_broker.sh
ssh ec2-user@${broker_public_ip} "./bin/${broker_script} >out.log 2>err.log &"
totail+=" -l 'ssh ec2-user@${broker_public_ip} tail -f *.log'"

for ip in ${producer_public_ip}; do
ssh ec2-user@${ip} './bin/start_producer.sh >out.log 2>err.log &'
totail+=" -l 'ssh ec2-user@${ip} tail -f *.log'"
done

for ip in ${consumer_public_ip}; do
ssh ec2-user@${ip} './bin/start_consumer.sh >out.log 2>err.log &'
totail+=" -l 'ssh ec2-user@${ip} tail -f *.log'"
done

echo totail

