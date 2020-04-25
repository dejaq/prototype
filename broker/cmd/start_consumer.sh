#!/usr/bin/env bash

go install github.com/dejaq/prototype/broker/cmd/dejaqcli-consumer
TOPIC=largetopic CONSUMER_ID=consumer1 TIMEOUT=99999999s dejaqcli-consumer
