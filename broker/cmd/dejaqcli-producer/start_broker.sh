#!/usr/bin/env bash

go install github.com/dejaq/prototype/broker/cmd/dejaqcli
cd ../dejaqcli/
 START_PRODUCERS=false START_CONSUMERS=false SEED=66 RUN_TIMEOUT=999999s dejaqcli