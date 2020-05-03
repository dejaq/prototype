#!/usr/bin/env bash

cd "$(dirname "$0")"
current_dir=$(pwd)

#populate the IP files
echo "fetching the IPs ..."
cd ./terraform/
privateIPs=$(terraform output -json CRDB-Private-Ips | jq -c -r '.[]')
privateIPsAsCSV=$(terraform output -json CRDB-Private-Ips | jq -c -r '. | @csv')

cd ${current_dir}

#cockroachdb should already be installed by user-data/crdb.sh terraform

provision_instance(){
    echo "provision the instance ${1}"
    ssh ec2-user@$1 cockroach start \
        --insecure \
        --advertise-addr=${1} \
        --join=${privateIPsAsCSV} \
        --cache=.35 \
        --max-sql-memory=.35 \
        --background
}

for ip in ${privateIPs}; do
    provision_instance ${ip}
done

echo "complete"
