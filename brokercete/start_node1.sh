#!/usr/bin/env bash

export CETE_ID=cete1
export CETE_RAFT_ADDRESS="127.0.0.1:8100"
export CETE_HTTP_ADDRESS="127.0.0.1:8102"
export CETE_GRPC_ADDRESS="127.0.0.1:8101"
export CETE_PEER_GRPC_ADDRESS="127.0.0.1:8101"
export CETE_DATA_DIRECTORY="/tmp/dejaq-data-node1"
go run main.go
