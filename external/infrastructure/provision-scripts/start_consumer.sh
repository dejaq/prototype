#!/usr/bin/env bash

cd "$(dirname "$0")"
BROKER_IP=$(<broker.privateip)
METRICS_PORT=9100 WORKERS_COUNT=36 TOPIC=largetopic CONSUMER_ID=consumer1 TIMEOUT=99999999s OVERSEER=${BROKER_IP}:9000 ./dejaqcli-consumer
