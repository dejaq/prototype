#!/usr/bin/env bash

go install github.com/dejaq/prototype/broker/cmd/dejaqcli-producer
TOPIC=topic_66_0;NAME="clitest";CONSTANT_TICK_DURATION=1s;CONSTANT_TICK_COUNT=100 dejaqcli-producer

