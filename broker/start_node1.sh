#!/usr/bin/env bash

export GOMAXPROCS=64 # https://github.com/dgraph-io/badger#are-there-any-go-specific-settings-that-i-should-use
export NODE_ID=cete1
export DEJAQ_ADDRESS="127.0.0.1:9000"
export RAFT_ADDRESS="127.0.0.1:8100"
export DATA_DIRECTORY="/tmp/dejaq-data-node1"
go run main.go
