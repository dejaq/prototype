#!/usr/bin/env bash
cd "$(dirname "$0")"

# exit when any command fails
set -e

#spawn the servers
cd ./terraform/
terraform init
terraform apply
