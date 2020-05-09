#!/usr/bin/env bash

export CETE_ID=cete2
export DEJAQ_ADDRESS="127.0.0.1:9100"
export CETE_RAFT_ADDRESS="127.0.0.1:8200"
export CETE_HTTP_ADDRESS="127.0.0.1:8202"
export CETE_GRPC_ADDRESS="127.0.0.1:8201"
export CETE_PEER_GRPC_ADDRESS="127.0.0.1:8101"
export CETE_DATA_DIRECTORY="/tmp/dejaq-data-node2"
go run main.go
