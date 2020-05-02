#!/usr/bin/env bash

cd "$(dirname "$0")"
BROKER_IP=$(<broker.privateip)
WORKERS_COUNT=12 TOPIC=largetopic NAME=clitest CONSTANT_TICK_DURATION=1s CONSTANT_TICK_COUNT=20000 TIMEOUT=99999999s  OVERSEER=${BROKER_IP}:9000 ./dejaqcli-producer
