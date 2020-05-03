#!/usr/bin/env bash


cd "$(dirname "$0")"
COCKROACH_HOSTNAME=$(<crdb.privateip)
STORAGE_TYPE=cockroach STORAGE_HOST=${COCKROACH_HOSTNAME}:26257 ./dejaqcli-broker