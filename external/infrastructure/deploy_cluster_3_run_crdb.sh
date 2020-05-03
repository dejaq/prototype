#!/usr/bin/env bash

cd "$(dirname "$0")"
current_dir=$(pwd)

#populate the IP files
echo "fetching the IPs ..."
cd ./terraform/
publicIPsTXT=$(terraform output -json CRDB-Public-Ips | jq -c -r '.[]')
privateIPsTXT=$(terraform output -json CRDB-Private-Ips | jq -c -r '.[]')
privateIPsAsCSV=$(terraform output -json CRDB-Private-Ips | jq -c -r '. | @csv')

cd ${current_dir}

#cockroachdb should already be installed by user-data/crdb.sh terraform

provision_instance(){
    publicIP=${1}
    privateIP=${2}
    echo "provision the instance ${publicIP} privateIP=${privateIP}"
    ssh ec2-user@${publicIP} "cockroach start --insecure --advertise-addr=${privateIP} --join=${privateIPsAsCSV} --cache=.35 --max-sql-memory=.35 --background >out.log 2>err.log &"
}

publicIPSArray=()
for ip in ${publicIPsTXT}; do
    publicIPSArray+=(${ip})
done

privateIPSArray=()
for ip in ${privateIPsTXT}; do
    privateIPSArray+=(${ip})
done

for i in "${!publicIPSArray[@]}"; do
    provision_instance ${publicIPSArray[i]} ${privateIPSArray[i]}
done

echo "complete"
