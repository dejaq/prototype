#!/usr/bin/env bash

cd "$(dirname "$0")"
current_dir=$(pwd)

#store what will be sent to all instances
rm -rf bin
mkdir bin

# exit when any command fails
set -e

#build the binaries
echo "building the binaries ..."
cd ../../broker/cmd/
export GOOS=linux GOARCH=amd64
go build -o ${current_dir}/bin/dejaqcli-broker dejaqcli-broker/main.go
go build -o ${current_dir}/bin/dejaqcli-producer dejaqcli-producer/main.go
go build -o ${current_dir}/bin/dejaqcli-consumer dejaqcli-consumer/main.go
cd ${current_dir}

#populate the IP files
echo "fetching the IPs ..."
cd ./terraform/
#this way the clients will know how to reach the broker
terraform output -json Broker-Private-Ips | jq -c -r  '.[0]' > ${current_dir}/bin/broker.privateip
#terraform output -json Redis-Dns | jq -c -r '.[0]' > ${current_dir}/bin/redis.privateip
terraform output -json CRDB-Private-Ips | jq -c -r '.[0]' > ${current_dir}/bin/crdb.privateip

broker_public_ip=$(terraform output -json Broker-Public-Ips | jq -c -r  '.[0]')
producer_public_ip=$(terraform output -json Producer-Public-Ips | jq -c -r  '.[]')
consumer_public_ip=$(terraform output -json Consumer-Public-Ips | jq -c -r  '.[]')
cockroach_public_ips=$(terraform output -json CRDB-Public-Ips | jq -c -r  '.[]')

cd ${current_dir}


echo "provision instances ..."
#make the archive with all the scripts, ips and binaries
cp ./provision-scripts/* bin/
zip -9 -m -q dejaq-linux bin/*

provision_instance(){
    echo "provision the instance ${1}"
    scp -o StrictHostKeyChecking=no dejaq-linux.zip ec2-user@$1:/home/ec2-user/
    ssh -o StrictHostKeyChecking=no ec2-user@$1 rm -rf bin/
    ssh -o StrictHostKeyChecking=no ec2-user@$1 unzip -q -o dejaq-linux.zip
    ssh -o StrictHostKeyChecking=no ec2-user@$1 rm dejaq-linux.zip
    ssh -o StrictHostKeyChecking=no ec2-user@$1 ./bin/install_utils_amazonlinux.sh
}

if [[ ! -z ${broker_public_ip} ]]; then
    provision_instance ${broker_public_ip}
fi

if [[ ! -z ${producer_public_ip} ]]; then
    for ip in ${producer_public_ip}; do
        provision_instance ${ip}
    done
fi

if [[ ! -z ${consumer_public_ip} ]]; then
    for ip in ${consumer_public_ip}; do
        provision_instance ${ip}
    done
fi

if [[ ! -z ${cockroach_public_ips} ]]; then
    for ip in ${cockroach_public_ips}; do
        provision_instance ${ip}
    done
fi

rm dejaq-linux.zip

echo "complete"
echo "broker: ssh ec2-user@${broker_public_ip}"
echo "producer: ssh ec2-user@${producer_public_ip}"
echo "consumer: ssh ec2-user@${consumer_public_ip}"
