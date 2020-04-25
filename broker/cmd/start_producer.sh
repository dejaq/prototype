#!/usr/bin/env bash

go install github.com/dejaq/prototype/broker/cmd/dejaqcli-producer
TOPIC=largetopic NAME=clitest CONSTANT_TICK_DURATION=1s SINGLE_BURST_EVENTS=20000 dejaqcli-producer

