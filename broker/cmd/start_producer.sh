#!/usr/bin/env bash

go install github.com/dejaq/prototype/broker/cmd/dejaqcli-producer
TOPIC=largetopic NAME="clitest" CONSTANT_TICK_DURATION=1s CONSTANT_TICK_COUNT=20000 dejaqcli-producer
