#!/usr/bin/env bash

go install github.com/dejaq/prototype/broker/cmd/dejaqcli-consumer
TOPIC=largetopic CONSUMER_ID=consumer1 STOP_AFTER=10 dejaqcli-consumer
